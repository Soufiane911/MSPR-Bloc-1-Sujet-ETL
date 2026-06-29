import { useEffect, useMemo, useState } from "react";
import { loadDashboardData, loadAviationData } from "./api";
import Sidebar from "./components/Sidebar";
import Tabs from "./components/Tabs";
import KpiBar from "./components/KpiBar";
import OverviewTab from "./tabs/OverviewTab";
import TrajetsTab from "./tabs/TrajetsTab";
import DayNightTab from "./tabs/DayNightTab";
import NetworkTab from "./tabs/NetworkTab";
import MapTab from "./tabs/MapTab";
import QualityTab from "./tabs/QualityTab";
import AviationPage from "./pages/AviationPage";
import DemoPredictPage from "./pages/DemoPredictPage";

export default function App() {
  const [activeTab, setActiveTab] = useState("overview");
  const [filters, setFilters] = useState({ country: "Tous", trainType: "Tous", distanceMin: 0, distanceMax: 2000 });
  const [state, setState] = useState({ loading: true, error: "", data: null });

  useEffect(() => {
    let mounted = true;
    setState((s) => ({ ...s, loading: true, error: "" }));
    Promise.allSettled([loadDashboardData(filters.country, filters.trainType), loadAviationData(20)])
      .then(([dashboardResult, aviationResult]) => {
        if (dashboardResult.status === "rejected") {
          throw dashboardResult.reason;
        }

        const combined = {
          ...dashboardResult.value,
          aviation: aviationResult.status === "fulfilled" ? aviationResult.value : null,
        };
        mounted && setState({ loading: false, error: "", data: combined });
      })
      .catch((err) => mounted && setState({ loading: false, error: err.message, data: null }));
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

  if (typeof window !== "undefined" && window.location.pathname.startsWith("/aviation")) {
    return <AviationPage />;
  }

  if (typeof window !== "undefined" && window.location.pathname.startsWith("/demo-predict")) {
    return <DemoPredictPage />;
  }

  return (
    <>
      <a className="skip-link" href="#contenu-principal">Aller au contenu</a>
      <main id="contenu-principal" role="main" className="app-shell">
        <Sidebar countries={countries} filters={filters} setFilters={setFilters} maxDistance={maxDistance} />
        <section className="content">
          <header>
            <h1>ObRail Europe</h1>
            <p>Analyse de données ferroviaires et aériennes</p>
            <p style={{ marginTop: 8 }}>
              <a href="/aviation">Voir les statistiques Aviation →</a>
              {" | "}
              <a href="/demo-predict">Démo IA /predict →</a>
            </p>
          </header>

          {state.loading && <div className="card">Chargement des donnees...</div>}
          {state.error && <div className="card error">Erreur: {state.error}</div>}

          {!state.loading && !state.error && d && (
            <>
              <KpiBar kpis={kpis} />
              <Tabs active={activeTab} setActive={setActiveTab} />

              {activeTab === "overview" && <OverviewTab trains={d.trains} byCountry={d.byCountry} />}
              {activeTab === "trajets" && <TrajetsTab trajets={d.trajets} />}
              {activeTab === "daynight" && <DayNightTab dayNight={d.dayNight} />}
              {activeTab === "network" && <NetworkTab routes={d.routes} />}
              {activeTab === "map" && <MapTab stations={d.stations} />}
              {activeTab === "quality" && <QualityTab quality={d.quality} schedules={schedules} />}
            </>
          )}
        </section>
      </main>
    </>
  );
}
