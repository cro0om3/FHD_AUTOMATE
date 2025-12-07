
import os
import hmac
import hashlib
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
import datetime as dt

import pandas as pd
from fastapi import FastAPI, Request, Form, UploadFile, File, Depends, HTTPException, status
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from genesys_to_agent_template import run_from_paths

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
UPLOADS_DIR = BASE_DIR / "uploads"
REPORTS_DIR = BASE_DIR / "reports"

SECRET_KEY = "change-this-secret-in-production"
SESSION_COOKIE_NAME = "fhd_session"

USERS = {
    "admin": hashlib.sha256("admin123".encode()).hexdigest()
}

app = FastAPI(title="FHD Automated Reporting System")

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "app" / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "app" / "templates"))

def sign_value(value: str) -> str:
    sig = hmac.new(SECRET_KEY.encode(), value.encode(), hashlib.sha256).hexdigest()
    return f"{value}|{sig}"

def verify_signed_value(signed: str) -> Optional[str]:
    try:
        value, sig = signed.rsplit("|", 1)
    except ValueError:
        return None
    expected_sig = hmac.new(SECRET_KEY.encode(), value.encode(), hashlib.sha256).hexdigest()
    if hmac.compare_digest(sig, expected_sig):
        return value
    return None

def get_current_user(request: Request) -> Optional[str]:
    cookie = request.cookies.get(SESSION_COOKIE_NAME)
    if not cookie:
        return None
    username = verify_signed_value(cookie)
    return username

async def require_user(request: Request) -> str:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=status.HTTP_302_FOUND, headers={"Location": "/login"})
    return user

def load_reports_index() -> list:
    index_path = DATA_DIR / "reports_index.json"
    if not index_path.exists():
        return []
    try:
        return json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        return []

def save_reports_index(items: list) -> None:
    index_path = DATA_DIR / "reports_index.json"
    index_path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    return templates.TemplateResponse("login.html", {"request": request, "error": None, "user": None})

@app.post("/login", response_class=HTMLResponse)
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    hashed = hashlib.sha256(password.encode()).hexdigest()
    stored = USERS.get(username)
    if not stored or stored != hashed:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid username or password.", "user": None},
            status_code=status.HTTP_401_UNAUTHORIZED,
        )
    response = RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=sign_value(username),
        httponly=True,
        secure=False,
        samesite="lax",
    )
    return response

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, user: str = Depends(require_user)):
    reports = load_reports_index()
    recent_reports = sorted(reports, key=lambda r: r.get("created_at", ""), reverse=True)[:5]

    stats_path = DATA_DIR / "dashboard_stats.json"
    stats: Dict[str, Any] = {}
    if stats_path.exists():
        try:
            stats = json.loads(stats_path.read_text(encoding="utf-8"))
        except Exception:
            stats = {}

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": user,
            "recent_reports": recent_reports,
            "stats": stats,
        },
    )

@app.post("/upload", response_class=HTMLResponse)
async def upload_file(
    request: Request,
    user: str = Depends(require_user),
    files: List[UploadFile] = File(...)
):
    """Upload one or more Genesys CSV files and generate an Agent Productivity report using the Excel template."""
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    saved_files: list[dict] = []
    for uf in files:
        original = uf.filename
        safe_name = original.replace(" ", "_")
        dest = UPLOADS_DIR / safe_name
        with dest.open("wb") as f:
            f.write(await uf.read())
        saved_files.append({"original": original, "path": dest})

    # Detect performance, status and booking CSVs by filename
    perf_path = None
    status_path = None
    booking_path = None
    for item in saved_files:
        lower = item["original"].lower()
        if "performance" in lower and "summary" in lower and perf_path is None:
            perf_path = item["path"]
        if "status" in lower and "summary" in lower and status_path is None:
            status_path = item["path"]
        if "booking" in lower and booking_path is None:
            booking_path = item["path"]

    if perf_path is None or status_path is None:
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "user": user,
                "message": "Could not find both Agent Performance Summary and Agent Status Summary CSV files in this upload. Please include both.",
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )

    template_path = BASE_DIR / "Agent Report Template.xlsx"
    if not template_path.exists():
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "user": user,
                "message": "Agent Report Template.xlsx not found in project root. Please copy your template file there.",
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    today_str = dt.date.today().isoformat()
    out_xlsx = REPORTS_DIR / f"agent_productivity_{today_str}.xlsx"
    out_csv = REPORTS_DIR / f"agent_productivity_{today_str}.csv"

    try:
        run_from_paths(perf_path, status_path, template_path, out_xlsx, out_csv, booking=booking_path)
    except Exception as e:
        return templates.TemplateResponse(
            "upload_error.html",
            {
                "request": request,
                "user": user,
                "message": f"Error while generating report: {e}",
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    reports = load_reports_index()
    report_entry = {
        "id": len(reports) + 1,
        "original_file": ", ".join([f["original"] for f in saved_files]),
        "stored_file": ", ".join([str(f["path"].name) for f in saved_files]),
        "report_file": str(out_xlsx.name),
        "created_by": user,
        "created_at": dt.datetime.utcnow().isoformat() + "Z",
    }
    reports.append(report_entry)
    save_reports_index(reports)

    stats: Dict[str, Any] = {}
    try:
        df_stats = pd.read_csv(out_csv)
        stats["total_reports"] = len(reports)
        stats["last_report_date"] = today_str
        if "Agent Name" in df_stats.columns:
            stats["total_agents"] = int(df_stats["Agent Name"].nunique())
        if "Total Answered Calls" in df_stats.columns:
            stats["total_answered"] = int(df_stats["Total Answered Calls"].sum())
        if "Outbound Calls" in df_stats.columns:
            stats["total_outbound"] = int(df_stats["Outbound Calls"].sum())
        if "Total Inbound Booking" in df_stats.columns:
            stats["total_bookings"] = int(df_stats["Total Inbound Booking"].sum())
        if "Avg Handle Time sec" in df_stats.columns and df_stats["Avg Handle Time sec"].notna().any():
            aht_sec = float(df_stats["Avg Handle Time sec"].mean())
            stats["avg_aht_sec"] = round(aht_sec, 1)
            stats["avg_aht_min"] = round(aht_sec / 60.0, 1)
    except Exception:
        stats.setdefault("total_reports", len(reports))
        stats.setdefault("last_report_date", today_str)

    stats_path = DATA_DIR / "dashboard_stats.json"
    stats_path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")

    return templates.TemplateResponse(
        "upload_success.html",
        {
            "request": request,
            "user": user,
            "original_name": report_entry["original_file"],
            "report_entry": report_entry,
        },
    )

@app.get("/reports", response_class=HTMLResponse)
async def list_reports(request: Request, user: str = Depends(require_user)):
    reports = load_reports_index()
    reports = sorted(reports, key=lambda r: r.get("created_at", ""), reverse=True)
    return templates.TemplateResponse(
        "reports.html", {"request": request, "user": user, "reports": reports}
    )

@app.get("/reports/{report_id}", response_class=HTMLResponse)
async def view_report(request: Request, report_id: int, user: str = Depends(require_user)):
    reports = load_reports_index()
    report = next((r for r in reports if r.get("id") == report_id), None)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    return templates.TemplateResponse(
        "report_viewer.html",
        {
            "request": request,
            "user": user,
            "report": report,
        },
    )

@app.get("/api/reports")
async def api_reports(user: str = Depends(require_user)) -> Dict[str, Any]:
    reports = load_reports_index()
    return {"count": len(reports), "items": reports}

@app.get("/genesys-dashboard", response_class=HTMLResponse)
async def genesys_dashboard(request: Request, user: str = Depends(require_user)):
    genesys_url = "https://apps.mec1.pure.cloud/directory/#/analytics/dashboards/84b9bc9e-7539-4f25-9c3c-2f02fe9481d3?tabId=6309d429-44b6-479d-b1a7-adfd12289238"
    return templates.TemplateResponse(
        "genesys_dashboard.html",
        {"request": request, "user": user, "genesys_url": genesys_url},
    )
