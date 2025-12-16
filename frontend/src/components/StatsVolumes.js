import { useEffect, useState, useCallback } from "react";

function StatsVolumes({ apiUrl }) {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchStats = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${apiUrl}/stats/volumes`);
      if (!response.ok) {
        throw new Error("Failed to fetch stats");
      }
      const data = await response.json();
      setStats(data);
    } catch (err) {
      setError("Error loading stats: " + err.message);
    } finally {
      setLoading(false);
    }
  }, [apiUrl]);

  useEffect(() => {
    fetchStats();
  }, [fetchStats]);

  if (loading) return <div className="loading">Chargement des statistiques...</div>;

  if (error) return <div className="error">{error}</div>;

  if (!stats) return <div className="loading">Pas de données</div>;

  return (
    <div className="stats-container">
      <div className="stats-summary">
        <div className="summary-card">
          <div className="summary-label">Total Agences</div>
          <div className="summary-value">{stats.summary.totalAgencies}</div>
        </div>
        <div className="summary-card">
          <div className="summary-label">Total Trajets</div>
          <div className="summary-value">{stats.summary.totalTrips}</div>
        </div>
      </div>

      {stats.byAgency && stats.byAgency.map((agency) => (
        <div key={agency.agencyId} className="agency-stats">
          <div className="agency-header">
            🏢 {agency.agencyName}
          </div>
          <div className="agency-body">
            {agency.dayTrips && (
              <>
                <h4 style={{ color: "#333", marginBottom: "10px" }}>☀️ Trajets Jour</h4>
                <div className="trip-type">
                  <div className="trip-stat">
                    <div className="trip-stat-label">Nombre</div>
                    <div className="trip-stat-value">{agency.dayTrips.count}</div>
                  </div>
                  <div className="trip-stat">
                    <div className="trip-stat-label">Distance totale</div>
                    <div className="trip-stat-value">{agency.dayTrips.totalDistanceKm.toFixed(0)} km</div>
                  </div>
                  <div className="trip-stat">
                    <div className="trip-stat-label">CO₂ Total</div>
                    <div className="trip-stat-value">
                      {(agency.dayTrips.totalCo2G / 1000).toFixed(1)} kg
                    </div>
                  </div>
                  <div className="trip-stat">
                    <div className="trip-stat-label">CO₂/Passager</div>
                    <div className="trip-stat-value">
                      {agency.dayTrips.avgCo2PerPassengerG.toFixed(1)} g
                    </div>
                  </div>
                </div>
              </>
            )}

            {agency.nightTrips && (
              <>
                <h4 style={{ color: "#333", marginBottom: "10px", marginTop: "15px" }}>🌙 Trajets Nuit</h4>
                <div className="trip-type">
                  <div className="trip-stat">
                    <div className="trip-stat-label">Nombre</div>
                    <div className="trip-stat-value">{agency.nightTrips.count}</div>
                  </div>
                  <div className="trip-stat">
                    <div className="trip-stat-label">Distance totale</div>
                    <div className="trip-stat-value">{agency.nightTrips.totalDistanceKm.toFixed(0)} km</div>
                  </div>
                  <div className="trip-stat">
                    <div className="trip-stat-label">CO₂ Total</div>
                    <div className="trip-stat-value">
                      {(agency.nightTrips.totalCo2G / 1000).toFixed(1)} kg
                    </div>
                  </div>
                  <div className="trip-stat">
                    <div className="trip-stat-label">CO₂/Passager</div>
                    <div className="trip-stat-value">
                      {agency.nightTrips.avgCo2PerPassengerG.toFixed(1)} g
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      ))}

      <button
        onClick={fetchStats}
        style={{
          width: "100%",
          padding: "12px",
          background: "#667eea",
          color: "white",
          border: "none",
          borderRadius: "4px",
          cursor: "pointer",
          fontWeight: "bold",
          marginTop: "20px",
        }}
      >
        Rafraîchir
      </button>
    </div>
  );
}

export default StatsVolumes;
