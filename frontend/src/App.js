import { useState } from "react";
import "./App.css";
import TrajetsList from "./components/TrajetsList";
import StatsVolumes from "./components/StatsVolumes";
import HealthCheck from "./components/HealthCheck";
import ImportHelper from "./components/ImportHelper";

function App() {
  const [activeTab, setActiveTab] = useState("trajets");
  const apiUrl = "http://localhost:5001";

  return (
    <div className="app">
      <header className="app-header">
        <h1>Dashboard</h1>
        <p>Analyse des dessertes ferroviaires européennes</p>
      </header>

      <div className="tabs">
        <button
          className={`tab-button ${activeTab === "health" ? "active" : ""}`}
          onClick={() => setActiveTab("health")}
        >
          Health
        </button>
        <button
          className={`tab-button ${activeTab === "trajets" ? "active" : ""}`}
          onClick={() => setActiveTab("trajets")}
        >
          Trajets
        </button>
        <button
          className={`tab-button ${activeTab === "stats" ? "active" : ""}`}
          onClick={() => setActiveTab("stats")}
        >
          Statistiques
        </button>
        <button
          className={`tab-button ${activeTab === "import" ? "active" : ""}`}
          onClick={() => setActiveTab("import")}
        >
          Import
        </button>
      </div>

      <div className="tab-content">
        {activeTab === "health" && <HealthCheck apiUrl={apiUrl} />}
        {activeTab === "trajets" && <TrajetsList apiUrl={apiUrl} />}
        {activeTab === "stats" && <StatsVolumes apiUrl={apiUrl} />}
        {activeTab === "import" && <ImportHelper apiUrl={apiUrl} />}
      </div>
    </div>
  );
}

export default App;

