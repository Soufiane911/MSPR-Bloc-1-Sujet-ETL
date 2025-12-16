import { useCallback, useState } from "react";

export default function ImportHelper({ apiUrl }) {
  const [url, setUrl] = useState("");
  const [preflight, setPreflight] = useState(null);
  const [loading, setLoading] = useState(false);
  const [note, setNote] = useState(null);
  const [error, setError] = useState(null);

  const runPreflight = useCallback(async () => {
    setError(null);
    setNote(null);
    setPreflight(null);
    if (!url) {
      setError("Please enter a GTFS ZIP URL");
      return;
    }
    setLoading(true);
    try {
      const res = await fetch(`${apiUrl}/import/gtfs/preflight?url=${encodeURIComponent(url)}`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.details || data.error || "Preflight failed");
      setPreflight(data.summary || {});
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [apiUrl, url]);

  const importNow = useCallback(async () => {
    setError(null);
    setNote(null);
    setLoading(true);
    try {
      const res = await fetch(`${apiUrl}/import/gtfs?url=${encodeURIComponent(url)}`, { method: "POST" });
      const data = await res.json();
      if (!res.ok) throw new Error(data.details || data.error || "Import failed");
      setPreflight(data.preflight || null);
      setNote(data.note || null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [apiUrl, url]);

  return (
    <div className="trajets-container">
      <div className="trajet-card">
        <div className="trajet-header">
          <div>
            <div className="trajet-title">Importer un flux GTFS</div>
            <div className="trajet-agency">Vérifie d'abord le contenu avant d'importer</div>
          </div>
        </div>

        <div className="filters" style={{ marginTop: 16 }}>
          <div className="filter-group" style={{ flex: 1 }}>
            <label>URL du GTFS (.zip)</label>
            <input
              type="text"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
              placeholder="https://.../google_transit.zip"
              style={{ width: "100%" }}
            />
          </div>
          <div className="filter-group">
            <label>&nbsp;</label>
            <button onClick={runPreflight} disabled={loading}>Préflight</button>
          </div>
          <div className="filter-group">
            <label>&nbsp;</label>
            <button onClick={importNow} disabled={loading}>Importer</button>
          </div>
        </div>

        {loading && <div className="loading">Traitement en cours…</div>}
        {error && <div className="error">{error}</div>}
        {note && <div className="error">{note}</div>}

        {preflight && (
          <div style={{ marginTop: 12 }}>
            <div className="trajet-title" style={{ marginBottom: 8 }}>Résumé du flux</div>
            <div className="trajet-stats">
              <div className="stat-box">
                <div className="stat-label">Agencies</div>
                <div className="stat-value">{preflight.agencies?.rows ?? 0}</div>
              </div>
              <div className="stat-box">
                <div className="stat-label">Routes</div>
                <div className="stat-value">{preflight.routes?.rows ?? 0}</div>
              </div>
              <div className="stat-box">
                <div className="stat-label">Stops</div>
                <div className="stat-value">{preflight.stops?.rows ?? 0}</div>
              </div>
              <div className="stat-box">
                <div className="stat-label">Trips</div>
                <div className="stat-value">{preflight.trips?.rows ?? 0}</div>
              </div>
              <div className="stat-box">
                <div className="stat-label">Stop Times</div>
                <div className="stat-value">{preflight.stopTimes?.rows ?? 0}</div>
              </div>
              <div className="stat-box">
                <div className="stat-label">Calendar</div>
                <div className="stat-value">{preflight.calendar?.rows ?? 0} / {preflight.calendarDates?.rows ?? 0}</div>
              </div>
            </div>
            <div style={{ marginTop: 8 }}>
              {preflight.hasCoreSchedules ? (
                <div className="trajet-agency">✅ Schedules trouvés (trips + stop_times)</div>
              ) : (
                <div className="error">⚠️ Pas de schedules (trips/stop_times). Vous verrez les lignes et arrêts, mais pas de trajets.</div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
