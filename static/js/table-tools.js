document.addEventListener("DOMContentLoaded", () => {
  const buttons = document.querySelectorAll("[data-table-action]");

  const downloadCSV = (csvContent, filename) => {
    const blob = new Blob([csvContent], { type: "application/vnd.ms-excel" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const tableToCSV = (table) => {
    if (!table) return "";
    return Array.from(table.querySelectorAll("tr"))
      .map((row) =>
        Array.from(row.querySelectorAll("th, td"))
          .map((cell) => `"${cell.textContent.replace(/"/g, '""')}"`)
          .join(",")
      )
      .join("\n");
  };

  const printTable = (table, title) => {
    const styles = `
      <style>
        body { font-family: "Inter", system-ui, sans-serif; margin: 1rem; color: #0f172a; background: #fff; }
        table { width: 100%; border-collapse: collapse; margin-bottom: 1rem; }
        th, td { border: 1px solid #e2e8f0; padding: 0.5rem; text-align: left; font-size: 11px; }
        h1 { font-size: 1.25rem; margin-bottom: 0.5rem; }
      </style>
    `;
    const win = window.open("", "_blank");
    if (!win) return;
    win.document.write(`
      <html>
        <head>
          <title>${title}</title>
          ${styles}
        </head>
        <body>
          <h1>${title}</h1>
          ${table.outerHTML}
        </body>
      </html>
    `);
    win.document.close();
    win.focus();
    setTimeout(() => win.print(), 300);
  };

  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const action = button.dataset.tableAction;
      const targetSelector = button.dataset.tableTarget;
      const table = targetSelector ? document.querySelector(targetSelector) : null;
      if (!table) return;
      const title = button.dataset.tableTitle || document.title;
      if (action === "print" || action === "save-pdf") {
        printTable(table, title);
        return;
      }
      if (action === "export") {
        const csv = tableToCSV(table);
        if (!csv) return;
        const filename = `${title.replace(/\s+/g, "_").toLowerCase()}_${action}.csv`;
        downloadCSV(csv, filename);
      }
    });
  });
});
