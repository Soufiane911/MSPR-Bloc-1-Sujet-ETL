import { useCallback, useState, useEffect } from "react";

export default function ImportHelper({ apiUrl, onLoadingChange }) {
  const [url, setUrl] = useState("");
  const [preflight, setPreflight] = useState(null);
  const [loading, setLoading] = useState(false);
  const [note, setNote] = useState(null);
  const [error, setError] = useState(null);
  const [progress, setProgress] = useState([]);

  useEffect(() => {
    if (onLoadingChange) {
      onLoadingChange(loading);
    }
  }, [loading, onLoadingChange]);

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
    setProgress([]);
    if (!url) {
      setError("Please enter a GTFS ZIP URL");
      return;
    }
    setLoading(true);
    
    const eventSource = new EventSource(`${apiUrl}/import/gtfs/stream?url=${encodeURIComponent(url)}`);
    
    eventSource.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      
      if (msg.status === "downloading") {
        setProgress(prev => [...prev, "🔽 Downloading GTFS..."]);
      } else if (msg.status === "downloaded") {
        setProgress(prev => [...prev, `✓ Downloaded (${msg.data.files} files)`]);
      } else if (msg.status === "processing") {
        setProgress(prev => [...prev, `⚙️ Processing ${msg.data.file} (${msg.data.rows} rows)...`]);
      } else if (msg.status === "completed") {
        setProgress(prev => [...prev, `✓ ${msg.data.file}: ${msg.data.imported} imported`]);
      } else if (msg.status === "computing_stats") {
        setProgress(prev => [...prev, "📊 Computing statistics..."]);
      } else if (msg.status === "complete") {
        setProgress(prev => [...prev, `✅ Import complete! ${JSON.stringify(msg.data.stats)}`]);
        setLoading(false);
        eventSource.close();
      } else if (msg.status === "error") {
        setError(msg.data.message || "Import failed");
        setLoading(false);
        eventSource.close();
      }
    };
    
    eventSource.onerror = () => {
      setError("Connection lost");
      setLoading(false);
      eventSource.close();
    };
  }, [apiUrl, url]);

  return (
    <div className="trajets-container">
      <div className="trajet-card">
        <div className="trajet-header">
          <div>
            <div className="trajet-title">Importer un flux GTFS</div>
            <div className="trajet-agency">Vérifie d'abord le contenu avant d'importer</div>
          </div>
          <button 
            className="export-csv-button"
            onClick={() => window.open(`${apiUrl}/export/csv/flat`, '_blank')}
            title="Exporter pour PowerBI (CSV dénormalisé)"
          >
            📊 Export PowerBI
          </button>
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

        {loading && (
          <div className="loading-import">
            <div className="loading-spinner"></div>
            <div>Traitement en cours…</div>
          </div>
        )}
        {error && <div className="error">{error}</div>}
        {note && <div className="error">{note}</div>}

        {progress.length > 0 && (
          <div className="progress-container">
            <div className="trajet-title" style={{ marginBottom: 8 }}>Import Progress</div>
            <div className="progress-log">
              {progress.map((msg, idx) => (
                <div key={idx} className="progress-message">
                  <span className="progress-icon">📝</span>
                  {msg}
                </div>
              ))}
            </div>
          </div>
        )}

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
