import React, { useEffect, useState } from "react";
import { loadAviationData } from "../api";
import AviationTab from "../tabs/AviationTab";

export default function AviationPage() {
  const [state, setState] = useState({ loading: true, error: "", data: null });

  useEffect(() => {
    let mounted = true;
    setState({ loading: true, error: "", data: null });
    loadAviationData(100)
      .then((d) => mounted && setState({ loading: false, error: "", data: d }))
      .catch((err) => mounted && setState({ loading: false, error: err.message, data: null }));
    return () => (mounted = false);
  }, []);

  return (
    <main className="app-shell">
      <section className="content">
        <header>
          <h1>Aviation — ObRail Europe</h1>
          <p>Statistiques aériennes séparées du dashboard ferroviaire.</p>
          <p>
            <a href="/">← Retour au dashboard trains</a>
          </p>
        </header>

        {state.loading && <div className="card">Chargement des donnees aviation...</div>}
        {state.error && <div className="card error">Erreur: {state.error}</div>}

        {!state.loading && !state.error && state.data && (
          <AviationTab aviation={state.data} />
        )}
      </section>
    </main>
  );
}
