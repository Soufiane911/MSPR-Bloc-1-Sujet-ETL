import React, { useEffect, useMemo, useState } from "react";
import { loadWatchOverview, loadWatchItems, loadWatchSources, refreshWatch } from "../api";

function formatDate(value) {
  if (!value) return "-";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleString("fr-FR", { dateStyle: "medium", timeStyle: "short" });
}

export default function WatchPage() {
  const [loading, setLoading] = useState(true);
  const [itemsLoading, setItemsLoading] = useState(true);
  const [error, setError] = useState("");
  const [overview, setOverview] = useState(null);
  const [sources, setSources] = useState([]);
  const [items, setItems] = useState([]);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshResult, setRefreshResult] = useState(null);

  const [sourceFilter, setSourceFilter] = useState("all");
  const [tagFilter, setTagFilter] = useState("");
  const [search, setSearch] = useState("");

  const reload = async ({ keepFilters = true } = {}) => {
    setLoading(true);
    setItemsLoading(true);
    setError("");
    try {
      const [ov, src] = await Promise.all([loadWatchOverview(), loadWatchSources()]);
      const nextSource = keepFilters ? sourceFilter : "all";
      const nextTag = keepFilters ? tagFilter : "";
      const data = await loadWatchItems({ source: nextSource, tag: nextTag, limit: 120, offset: 0 });

      setOverview(ov);
      setSources(src);
      setItems(data);
      if (!keepFilters) {
        setSourceFilter("all");
        setTagFilter("");
      }
    } catch (err) {
      setError(err.message || "Erreur lors du chargement de la veille.");
    } finally {
      setLoading(false);
      setItemsLoading(false);
    }
  };

  useEffect(() => {
    reload({ keepFilters: true });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    let cancelled = false;
    setItemsLoading(true);
    setError("");
    loadWatchItems({ source: sourceFilter, tag: tagFilter, limit: 120, offset: 0 })
      .then((data) => {
        if (!cancelled) setItems(data);
      })
      .catch((err) => {
        if (!cancelled) setError(err.message || "Erreur de filtre");
      })
      .finally(() => {
        if (!cancelled) setItemsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [sourceFilter, tagFilter]);

  const onRefresh = async () => {
    setRefreshing(true);
    setRefreshResult(null);
    setError("");
    try {
      const result = await refreshWatch();
      setRefreshResult(result);
      await reload({ keepFilters: true });
    } catch (err) {
      setError(err.message || "Erreur pendant le refresh");
    } finally {
      setRefreshing(false);
    }
  };

  const sourceOptions = useMemo(() => {
    const names = (sources || []).map((s) => s.source);
    return ["all", ...names];
  }, [sources]);

  const filteredItems = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return items;
    return items.filter((it) => {
      const txt = `${it.title || ""} ${it.summary || ""} ${it.author || ""}`.toLowerCase();
      return txt.includes(q);
    });
  }, [items, search]);

  return (
    <main className="watch-shell">
      <section className="watch-topbar">
        <div>
          <h1>Veille Technologique</h1>
          <p>Sources agrégées: Feedly, Google Alerts, RSS, News API, Reddit, Hacker News.</p>
          <p>
            <a href="/">← Retour dashboard ferroviaire</a>
            {" | "}
            <a href="/aviation">Aviation</a>
          </p>
        </div>
        <button className="watch-refresh" type="button" onClick={onRefresh} disabled={refreshing}>
          {refreshing ? "Actualisation..." : "Actualiser les sources"}
        </button>
      </section>

      {refreshResult && (
        <section className="card watch-notice">
          <strong>Refresh terminé</strong>
          <span>
            {refreshResult.inserted} ajoutés, {refreshResult.skipped} ignorés
          </span>
        </section>
      )}

      {error && <section className="card error">Erreur: {error}</section>}

      <section className="watch-kpis">
        <article className="watch-kpi-card">
          <span>Total articles</span>
          <strong>{loading && !overview ? "..." : overview?.total_items ?? 0}</strong>
        </article>
        <article className="watch-kpi-card">
          <span>Dernière collecte</span>
          <strong>{loading && !overview ? "..." : formatDate(overview?.last_fetch)}</strong>
        </article>
        <article className="watch-kpi-card">
          <span>Sources actives</span>
          <strong>
            {loading && !overview ? "..." : (overview?.enabled_sources || []).length}
          </strong>
        </article>
      </section>

      <section className="watch-layout">
        <aside className="card watch-sidebar">
          <h3>Filtres</h3>
          <label>
            Source
            <select value={sourceFilter} onChange={(e) => setSourceFilter(e.target.value)}>
              {sourceOptions.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </label>

          <label>
            Tag
            <input
              type="text"
              placeholder="Ex: rss"
              value={tagFilter}
              onChange={(e) => setTagFilter(e.target.value)}
            />
          </label>

          <label>
            Recherche texte
            <input
              type="text"
              placeholder="Titre, résumé, auteur"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </label>

          <h3>Statut sources</h3>
          <ul className="watch-source-list">
            {(sources || []).map((s) => (
              <li key={s.source}>
                <span className={`watch-dot ${s.enabled ? "on" : "off"}`} />
                <strong>{s.source}</strong>
                <small>{s.item_count} items</small>
              </li>
            ))}
          </ul>
        </aside>

        <section className="card watch-feed">
          <h3>Flux ({filteredItems.length})</h3>
          {itemsLoading && <p className="muted">Chargement...</p>}

          {!itemsLoading && filteredItems.length === 0 && (
            <p className="muted">Aucun résultat pour ces filtres.</p>
          )}

          <div className="watch-items">
            {filteredItems.map((item) => (
              <article key={item.item_id} className="watch-item">
                <header>
                  <span className="watch-pill">{item.source}</span>
                  <time>{formatDate(item.published_at || item.fetched_at)}</time>
                </header>
                <h4>{item.title}</h4>
                {item.summary && <p>{item.summary.replace(/<[^>]+>/g, "").slice(0, 260)}...</p>}
                <footer>
                  <span>{item.author || "Source externe"}</span>
                  <a href={item.url} target="_blank" rel="noreferrer">
                    Ouvrir
                  </a>
                </footer>
              </article>
            ))}
          </div>
        </section>
      </section>
    </main>
  );
}
