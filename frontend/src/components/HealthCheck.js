import { useEffect, useState, useCallback } from "react";

function HealthCheck({ apiUrl }) {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const checkHealth = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`${apiUrl}/health`);
      const data = await response.json();
      setStatus(data);
    } catch (err) {
      setError("Unable to reach API: " + err.message);
      setStatus(null);
    } finally {
      setLoading(false);
    }
  }, [apiUrl]);

  useEffect(() => {
    checkHealth();
    const interval = setInterval(checkHealth, 5000);
    return () => clearInterval(interval);
  }, [checkHealth]);

  if (loading) return <div className="loading">Checking health...</div>;

  return (
    <div className="health-container">
      {status ? (
        <>
          <div className={`health-status ${status.status}`}>
            <div className="status-indicator"></div>
            <span>{status.status.toUpperCase()}</span>
          </div>
          <div className="health-details">
            <div className="detail-row">
              <span className="detail-label">Status:</span>
              <span className="detail-value">{status.status}</span>
            </div>
            <div className="detail-row">
              <span className="detail-label">Database:</span>
              <span className="detail-value">{status.database}</span>
            </div>
          </div>
          <button
            onClick={checkHealth}
            style={{
              width: "100%",
              padding: "10px",
              background: "#667eea",
              color: "white",
              border: "none",
              borderRadius: "4px",
              cursor: "pointer",
              fontWeight: "bold",
            }}
          >
            Refresh
          </button>
        </>
      ) : (
        <div className="error">{error}</div>
      )}
    </div>
  );
}

export default HealthCheck;
