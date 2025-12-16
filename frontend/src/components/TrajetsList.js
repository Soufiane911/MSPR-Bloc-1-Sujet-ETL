import { useEffect, useState, useCallback } from "react";

function TrajetsList({ apiUrl }) {
  const [trajets, setTrajets] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [total, setTotal] = useState(0);
  const [limit, setLimit] = useState(10);
  const [offset, setOffset] = useState(0);

  const [filterIsNight, setFilterIsNight] = useState("");
  const [filterAgencyId, setFilterAgencyId] = useState("");

  const fetchTrajets = useCallback(async (newOffset = 0) => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.append("limit", limit);
      params.append("offset", newOffset);
      if (filterIsNight) {
        params.append("is_night", filterIsNight);
      }
      if (filterAgencyId) {
        params.append("agency_id", filterAgencyId);
      }

      const response = await fetch(`${apiUrl}/trajets?${params}`);
      if (!response.ok) {
        throw new Error("Failed to fetch trajets");
      }
      const data = await response.json();
      setTrajets(data.trajets);
      setTotal(data.total);
      setOffset(newOffset);
    } catch (err) {
      setError("Error loading trajets: " + err.message);
    } finally {
      setLoading(false);
    }
  }, [apiUrl, limit, filterIsNight, filterAgencyId]);

  useEffect(() => {
    fetchTrajets(0);
  }, [fetchTrajets]);

  const nextPage = () => {
    if (offset + limit < total) {
      fetchTrajets(offset + limit);
    }
  };

  const prevPage = () => {
    if (offset > 0) {
      fetchTrajets(Math.max(0, offset - limit));
    }
  };

  return (
    <div className="trajets-container">
      <div className="filters">
        <div className="filter-group">
          <label>Service:</label>
          <select value={filterIsNight} onChange={(e) => setFilterIsNight(e.target.value)}>
            <option value="">Tous</option>
            <option value="false">Jour</option>
            <option value="true">Nuit</option>
          </select>
        </div>
        <div className="filter-group">
          <label>Agency ID:</label>
          <input
            type="number"
            value={filterAgencyId}
            onChange={(e) => setFilterAgencyId(e.target.value)}
            placeholder="ex: 1"
          />
        </div>
        <div className="filter-group">
          <label>Per page:</label>
          <select value={limit} onChange={(e) => setLimit(parseInt(e.target.value))}>
            <option value="5">5</option>
            <option value="10">10</option>
            <option value="20">20</option>
            <option value="50">50</option>
          </select>
        </div>
      </div>

      {error && <div className="error">{error}</div>}

      {loading && <div className="loading">Chargement des trajets...</div>}

      {!loading && trajets.length === 0 && (
        <div className="error">Aucun trajet trouvé. Utilisez l'onglet Import pour charger un flux GTFS avec des trips/stop_times.</div>
      )}

      {!loading && trajets.map((trajet) => (
        <div key={trajet.id} className="trajet-card">
          <div className="trajet-header">
            <div>
              <div className="trajet-title">{trajet.ligne.nomLong}</div>
              <div className="trajet-agency">🏢 {trajet.agence.nom}</div>
            </div>
            <div className={`trajet-badge ${trajet.trainDeNuit ? "badge-night" : "badge-day"}`}>
              {trajet.trainDeNuit ? "🌙 Nuit" : "☀️ Jour"}
            </div>
          </div>

          <div className="trajet-horaires">
            {trajet.horaires && trajet.horaires.map((h, idx) => (
              <div key={idx} className="horaire-item">
                <div className="horaire-time">
                  {h.departure || h.arrival}
                </div>
                <div className="horaire-station">{h.arret}</div>
              </div>
            ))}
          </div>

          {trajet.stats && (
            <div className="trajet-stats">
              <div className="stat-box">
                <div className="stat-label">Distance</div>
                <div className="stat-value">
                  {trajet.stats.distanceKm ? trajet.stats.distanceKm.toFixed(0) : "N/A"} km
                </div>
              </div>
              <div className="stat-box">
                <div className="stat-label">Durée</div>
                <div className="stat-value">
                  {trajet.stats.dureeMinutes ? Math.round(trajet.stats.dureeMinutes / 60) : "N/A"}h
                </div>
              </div>
              <div className="stat-box">
                <div className="stat-label">CO₂ Total</div>
                <div className="stat-value">
                  {trajet.stats.co2TotalG ? (trajet.stats.co2TotalG / 1000).toFixed(1) : "N/A"} kg
                </div>
              </div>
              <div className="stat-box">
                <div className="stat-label">CO₂/Passager</div>
                <div className="stat-value">
                  {trajet.stats.co2ParPassagerG ? trajet.stats.co2ParPassagerG.toFixed(1) : "N/A"} g
                </div>
              </div>
            </div>
          )}
        </div>
      ))}

      {!loading && trajets.length > 0 && (
        <div className="pagination">
          <button onClick={prevPage} disabled={offset === 0}>
            ← Précédent
          </button>
          <span className="current">
            {offset + 1} - {Math.min(offset + limit, total)} / {total}
          </span>
          <button onClick={nextPage} disabled={offset + limit >= total}>
            Suivant →
          </button>
        </div>
      )}
    </div>
  );
}

export default TrajetsList;
