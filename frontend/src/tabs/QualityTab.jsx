import { exportCsv, exportXlsx } from "../utils/export";

export default function QualityTab({ quality, schedules }) {
  return (
    <section className="grid-2">
      <article className="card">
        <h3>Qualite des donnees</h3>
        <table className="table">
          <thead>
            <tr>
              <th>Table</th>
              <th>Total</th>
            </tr>
          </thead>
          <tbody>
            {(quality || []).map((q) => (
              <tr key={q.table_name}>
                <td>{q.table_name}</td>
                <td>{q.total_records ?? 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </article>

      <article className="card">
        <h3>Export donnees</h3>
        <p className="muted">
          Export des dessertes filtrees pour conserver la meme capacite qu'avec Streamlit.
        </p>
        <div className="actions">
          <button type="button" onClick={() => exportCsv(schedules)}>
            Export CSV
          </button>
          <button type="button" onClick={() => exportXlsx(schedules)}>
            Export Excel
          </button>
        </div>
      </article>
    </section>
  );
}
