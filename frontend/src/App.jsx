import { useEffect, useMemo, useState } from "react";
import { loadDashboardData } from "./api";
import Sidebar from "./components/Sidebar";
import Tabs from "./components/Tabs";
import KpiBar from "./components/KpiBar";
import OverviewTab from "./tabs/OverviewTab";
import DayNightTab from "./tabs/DayNightTab";
import NetworkTab from "./tabs/NetworkTab";
import MapTab from "./tabs/MapTab";
import QualityTab from "./tabs/QualityTab";

export default function App() {
  const [activeTab, setActiveTab] = useState("overview");
  const [filters, setFilters] = useState({ country: "Tous", trainType: "Tous", distanceMin: 0, distanceMax: 2000 });
  const [state, setState] = useState({ loading: true, error: "", warnings: [], data: null });

  useEffect(() => {
    let mounted = true;
    setState((s) => ({ ...s, loading: true, error: "", warnings: [] }));
    loadDashboardData(filters.country, filters.trainType)
      .then(({ data, errors }) => {
        if (!mounted) return;
        if (errors.length > 0) {
          // Certains endpoints ont echoue mais d'autres sont disponibles
          setState({ loading: false, error: "", warnings: errors, data });
        } else {
          setState({ loading: false, error: "", warnings: [], data });
        }
      })
      .catch((err) => mounted && setState({ loading: false, error: err.message, warnings: [], data: null }));
    return () => {
      mounted = false;
    };
  }, [filters.country, filters.trainType]);

  const d = state.data;
  const schedules = useMemo(() => {
    const raw = d?.schedules || [];
    return raw.filter((s) => {
      const distance = Number(s.distance_km || 0);
      if (!distance) return true;
      return distance >= filters.distanceMin && distance <= filters.distanceMax;
    });
  }, [d, filters.distanceMin, filters.distanceMax]);

  const kpis = useMemo(() => ({
    trains: d?.trains?.length || 0,
    stations: d?.stations?.length || 0,
    operators: d?.operators?.length || 0,
    schedules: schedules.length,
    co2: schedules.reduce((sum, row) => sum + Number(row.distance_km || 0) * 0.21, 0),
  }), [d, schedules]);

  const countries = useMemo(() => {
    const set = new Set((d?.stations || []).map((s) => s.country).filter(Boolean));
    return Array.from(set).sort();
  }, [d]);

  const maxDistance = useMemo(() => {
    const max = Math.max(0, ...((d?.schedules || []).map((s) => Number(s.distance_km || 0))));
    return Math.ceil(max || 2000);
  }, [d]);

  return (
    <>
      <a href="#contenu-principal" className="skip-link">
        Aller au contenu
      </a>
      <Sidebar countries={countries} filters={filters} setFilters={setFilters} maxDistance={maxDistance} />
      <main className="app-shell" id="contenu-principal" role="main" tabIndex={-1}>
        <section className="content">
          <header>
            <h1>ObRail Europe</h1>
            <p>Dashboard React remplacant Streamlit avec fonctionnalites equivalentes.</p>
          </header>

          {state.loading && <div className="card">Chargement des donnees...</div>}
          {state.error && <div className="card error">Erreur: {state.error}</div>}

          {state.warnings.length > 0 && (
            <div className="card" style={{ borderColor: "#f39b2f", background: "#fff8dc" }}>
              <strong style={{ color: "#b37400" }}>Avertissement : certains endpoints sont indisponibles</strong>
              <ul style={{ margin: "0.5rem 0 0 1rem", fontSize: "0.85rem", color: "#607086" }}>
                {state.warnings.map((w, i) => (
                  <li key={i}>{w}</li>
                ))}
              </ul>
            </div>
          )}

          {!state.loading && !state.error && d && (
            <>
              <KpiBar kpis={kpis} />
              <Tabs active={activeTab} setActive={setActiveTab} />

              {activeTab === "overview" && <OverviewTab trains={d.trains || []} byCountry={d.byCountry || []} />}
              {activeTab === "daynight" && <DayNightTab dayNight={d.dayNight || []} />}
              {activeTab === "network" && <NetworkTab routes={d.routes || []} />}
              {activeTab === "map" && <MapTab stations={d.stations || []} />}
              {activeTab === "quality" && <QualityTab quality={d.quality || []} schedules={schedules} />}
            </>
          )}
        </section>
      </main>
    </>
  );
}
