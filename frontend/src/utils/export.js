import * as XLSX from "xlsx";

export function exportCsv(rows, filename = "obrail_export.csv") {
  if (!rows?.length) return;
  const headers = Object.keys(rows[0]);
  const lines = [headers.join(",")].concat(
    rows.map((row) => headers.map((h) => JSON.stringify(row[h] ?? "")).join(","))
  );
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}

export function exportXlsx(rows, filename = "obrail_export.xlsx") {
  if (!rows?.length) return;
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Donnees");
  XLSX.writeFile(wb, filename);
}
