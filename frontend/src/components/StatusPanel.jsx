function formatTimestamp(value) {
  if (!value) return "Verification en attente";
  return new Intl.DateTimeFormat("fr-FR", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(value);
}

function getStatusTone(status) {
  if (status === "healthy") {
    return {
      label: "Operationnel",
      className: "is-healthy",
      message: "API et base de donnees repondent correctement.",
    };
  }

  if (status === "degraded") {
    return {
      label: "Degrade",
      className: "is-degraded",
      message: "Le service reste accessible, mais une dependance repond partiellement.",
    };
  }

  return {
    label: "Indisponible",
    className: "is-down",
    message: "Verification impossible ou service temporairement indisponible.",
  };
}

export default function StatusPanel({ health, loading, error, onRefresh }) {
  const tone = getStatusTone(health?.status);

  return (
    <section className="status-panel card" aria-label="Etat du service">
      <div className="status-panel-header">
        <div>
          <p className="eyebrow">Supervision</p>
          <h2>Etat du service</h2>
          <p className="muted">
            Vue rapide de disponibilite de l&apos;API ObRail pour la soutenance et les verifications de recette.
          </p>
        </div>
        <button type="button" className="secondary-button" onClick={onRefresh} disabled={loading}>
          {loading ? "Actualisation..." : "Actualiser"}
        </button>
      </div>

      <div className="status-grid">
        <article className={`status-card ${tone.className}`}>
          <span className="status-label">Service applicatif</span>
          <strong>{tone.label}</strong>
          <p>{error ? "Le statut n'a pas pu etre recupere automatiquement." : tone.message}</p>
        </article>

        <article className="status-card">
          <span className="status-label">API</span>
          <strong>{health?.api === "running" ? "Active" : "Non verifiee"}</strong>
          <p>Endpoint de sante interroge via `/health`.</p>
        </article>

        <article className="status-card">
          <span className="status-label">Base de donnees</span>
          <strong>{health?.database === "connected" ? "Connectee" : "A verifier"}</strong>
          <p>{health?.database === "connected" ? "Connexion validee." : "Une verification manuelle peut etre necessaire."}</p>
        </article>

        <article className="status-card">
          <span className="status-label">Derniere verification</span>
          <strong>{formatTimestamp(health?.checkedAt)}</strong>
          <p>{error || "Pensez a croiser ce statut avec Grafana en cas d'incident."}</p>
        </article>
      </div>
    </section>
  );
}
