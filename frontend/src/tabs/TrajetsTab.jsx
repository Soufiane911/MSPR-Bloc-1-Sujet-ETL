function formatDuration(minutes) {
  if (!minutes) return "-";
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  if (!hours) return `${rest} min`;
  return `${hours} h ${String(rest).padStart(2, "0")}`;
}

function formatDistance(distance) {
  if (!distance) return "-";
  return `${Math.round(distance)} km`;
}

function formatTrainType(type) {
  return type === "night" ? "Nuit" : "Jour";
}

export default function TrajetsTab({ trajets }) {
  const rows = (trajets || []).slice(0, 100);

  return (
    <section className="card">
      <div className="section-heading">
        <div>
          <h3>Consultation des trajets</h3>
          <p className="muted">{rows.length} trajets affiches selon les filtres actifs. Les 100 premiers resultats sont presentes pour conserver une lecture fluide.</p>
        </div>
      </div>

      {!rows.length ? (
        <div className="empty-state">Aucun trajet ne correspond aux filtres selectionnes. Elargissez la distance, revenez sur "Tous" ou actualisez les donnees.</div>
      ) : (
        <div className="table-wrap">
          <table className="table" aria-label="Tableau des trajets ferroviaires">
            <thead>
              <tr>
                <th>Origine</th>
                <th>Destination</th>
                <th>Operateur</th>
                <th>Type</th>
                <th>Duree</th>
                <th>Distance</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((trajet) => (
                <tr key={trajet.trajet_id}>
                  <td>
                    <strong>{trajet.origin}</strong>
                    <span>{trajet.origin_country}</span>
                  </td>
                  <td>
                    <strong>{trajet.destination}</strong>
                    <span>{trajet.destination_country}</span>
                  </td>
                  <td>{trajet.operator_name}</td>
                  <td>
                    <span className={`type-badge ${trajet.train_type}`}>
                      {formatTrainType(trajet.train_type)}
                    </span>
                  </td>
                  <td>{formatDuration(trajet.duration_min)}</td>
                  <td>{formatDistance(trajet.distance_km)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
