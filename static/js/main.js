(() => {
    const deleteButtons = document.querySelectorAll("[data-confirm-delete='true']");
    deleteButtons.forEach((button) => {
        button.addEventListener("click", (event) => {
            const approved = window.confirm("Delete this record? This action cannot be undone.");
            if (!approved) {
                event.preventDefault();
            }
        });
    });

    const autoSubmitFields = document.querySelectorAll("[data-auto-submit='true']");
    autoSubmitFields.forEach((field) => {
        field.addEventListener("change", () => {
            const form = field.closest("form");
            if (form) {
                form.submit();
            }
        });
    });

    const timelineBars = document.querySelectorAll(".trend-bar, .trend-strip div");
    timelineBars.forEach((bar, index) => {
        bar.style.animationDelay = `${index * 55}ms`;
    });
})();
