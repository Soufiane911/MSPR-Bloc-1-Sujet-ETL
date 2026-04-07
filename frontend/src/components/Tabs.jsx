const TABS = [
  ["overview", "Vue d'ensemble"],
  ["daynight", "Comparaison Jour/Nuit"],
  ["network", "Reseau & Distance"],
  ["map", "Carte"],
  ["quality", "Qualite & Export"],
];

export default function Tabs({ active, setActive }) {
  return (
    <nav className="tabs">
      {TABS.map(([id, label]) => (
        <button
          type="button"
          key={id}
          onClick={() => setActive(id)}
          className={active === id ? "active" : ""}
        >
          {label}
        </button>
      ))}
    </nav>
  );
}
