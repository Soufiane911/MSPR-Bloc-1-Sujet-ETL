import React, { useEffect, useState } from "react";
import { loadAviationData } from "../api";

export default function AviationPage() {
  const [state, setState] = useState({ loading: true, error: "", data: null });

  useEffect(() => {
    let mounted = true;
    setState({ loading: true, error: "", data: null });
    loadAviationData(100)
      .then((d) => mounted && setState({ loading: false, error: "", data: d }))
      .catch((err) => mounted && setState({ loading: false, error: err.message, data: null }));
    return () => {
      mounted = false;
    };
  }, []);

  const d = state.data;

  return (
    <main className="app-shell">
      <section className="content">
        <header>
          <h1>Aviation — ObRail Europe</h1>
          <p>Statistiques aériennes séparées du dashboard ferroviaire.</p>
          <p>
            <a href="/">← Retour au dashboard trains</a>
            {" | "}
            <a href="/demo-predict">Démo IA /predict</a>
          </p>
        </header>

        {state.loading && <div className="card">Chargement des donnees aviation...</div>}
        {state.error && <div className="card error">Erreur: {state.error}</div>}

        {!state.loading && !state.error && d && (
          <>
            <section className="kpi-grid">
              <div className="kpi"><span>Compagnies</span><strong>{d.summary?.airlines ?? 0}</strong></div>
              <div className="kpi"><span>Aéroports</span><strong>{d.summary?.airports ?? 0}</strong></div>
              <div className="kpi"><span>Routes</span><strong>{d.summary?.flights ?? 0}</strong></div>
              <div className="kpi"><span>Pays</span><strong>{d.summary?.countries ?? 0}</strong></div>
            </section>

            <section className="card">
              <h3>Top routes (aperçu)</h3>
              <table className="table">
                <thead>
                  <tr>
                    <th>Origine</th>
                    <th>Destination</th>
                    <th>Distance moy.</th>
                  </tr>
                </thead>
                <tbody>
                  {(d.topRoutes || []).slice(0, 12).map((r, idx) => (
                    <tr key={`${r.origin_iata || r.origin_name}-${r.destination_iata || r.destination_name}-${idx}`}>
                      <td>{r.origin_iata || r.origin_name}</td>
                      <td>{r.destination_iata || r.destination_name}</td>
                      <td>{Math.round(Number(r.avg_distance_km || 0))} km</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          </>
        )}
      </section>
    </main>
  );
}
