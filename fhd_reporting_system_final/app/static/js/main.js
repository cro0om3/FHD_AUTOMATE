
document.addEventListener("DOMContentLoaded", () => {
    const root = document.documentElement;
    const toggle = document.getElementById("themeToggle");

    const stored = localStorage.getItem("fhd-theme");
    if (stored === "light") {
        root.setAttribute("data-theme", "light");
        if (toggle) toggle.checked = false;
    } else {
        root.setAttribute("data-theme", "dark");
        if (toggle) toggle.checked = true;
    }

    if (toggle) {
        toggle.addEventListener("change", () => {
            const theme = toggle.checked ? "dark" : "light";
            root.setAttribute("data-theme", theme);
            localStorage.setItem("fhd-theme", theme);
        });
    }
});
