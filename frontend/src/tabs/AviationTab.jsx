import React from "react";

export default function AviationTab({ aviation }) {
  const a = aviation || {};
  return (
    <div>
      <section className="card">
        <h3>Résumé</h3>
        <div style={{ display: "flex", gap: "1rem" }}>
          <div className="kpi">Compagnies: {a.summary?.airlines ?? 0}</div>
          <div className="kpi">Aéroports: {a.summary?.airports ?? 0}</div>
          <div className="kpi">Lignes: {a.summary?.flights ?? 0}</div>
          <div className="kpi">Pays: {a.summary?.countries ?? 0}</div>
        </div>
      </section>

      <section className="card">
        <h3>Top routes</h3>
        <table className="table">
          <thead>
            <tr>
              <th>Origine</th>
              <th>Destination</th>
              <th>Freq.</th>
              <th>Avg km</th>
            </tr>
          </thead>
          <tbody>
            {(a.topRoutes || []).map((r) => (
              <tr key={`${r.origin_id}-${r.destination_id}`}>
                <td>{r.origin_name} ({r.origin_iata})</td>
                <td>{r.destination_name} ({r.destination_iata})</td>
                <td>{r.frequency}</td>
                <td>{r.avg_distance_km}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="card">
        <h3>Top aéroports</h3>
        <table className="table">
          <thead>
            <tr>
              <th>Aéroport</th>
              <th>Ville</th>
              <th>Mouvements</th>
              <th>Departures</th>
              <th>Arrivals</th>
            </tr>
          </thead>
          <tbody>
            {(a.topAirports || []).map((ap) => (
              <tr key={ap.airport_id}>
                <td>{ap.name} ({ap.iata})</td>
                <td>{ap.city}</td>
                <td>{ap.movements}</td>
                <td>{ap.departures}</td>
                <td>{ap.arrivals}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="card">
        <h3>Compagnies</h3>
        <table className="table">
          <thead>
            <tr>
              <th>Compagnie</th>
              <th>Pays</th>
              <th>Routes</th>
              <th>Instances</th>
            </tr>
          </thead>
          <tbody>
            {(a.airlineSummary || []).map((al) => (
              <tr key={al.airline_id}>
                <td>{al.airline_name}</td>
                <td>{al.country}</td>
                <td>{al.nb_routes}</td>
                <td>{al.nb_instances}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}
