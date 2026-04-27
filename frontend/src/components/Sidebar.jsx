export default function Sidebar({ countries, filters, setFilters, maxDistance }) {
  return (
    <aside className="sidebar">
      <h2>ObRail Europe</h2>
      <p className="muted">Filtres</p>

      <label htmlFor="filter-country">Pays</label>
      <select
        id="filter-country"
        value={filters.country}
        onChange={(e) => setFilters((v) => ({ ...v, country: e.target.value }))}
      >
        <option>Tous</option>
        {countries.map((country) => (
          <option key={country} value={country}>
            {country}
          </option>
        ))}
      </select>

      <label htmlFor="filter-train-type">Type de train</label>
      <select
        id="filter-train-type"
        value={filters.trainType}
        onChange={(e) => setFilters((v) => ({ ...v, trainType: e.target.value }))}
      >
        <option>Tous</option>
        <option>Jour</option>
        <option>Nuit</option>
      </select>

      <label>Distance min/max (km)</label>
      <div className="range-row">
        <input
          id="filter-distance-min"
          aria-label="Distance minimale"
          type="number"
          min={0}
          value={filters.distanceMin}
          onChange={(e) => setFilters((v) => ({ ...v, distanceMin: Number(e.target.value) || 0 }))}
        />
        <input
          id="filter-distance-max"
          aria-label="Distance maximale"
          type="number"
          min={0}
          max={maxDistance}
          value={filters.distanceMax}
          onChange={(e) => setFilters((v) => ({ ...v, distanceMax: Number(e.target.value) || maxDistance }))}
        />
      </div>

      <p className="hint">
        Donnees ferroviaires europeennes.
      </p>
    </aside>
  );
}
