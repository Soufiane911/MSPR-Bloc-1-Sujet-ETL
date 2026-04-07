function formatCo2(total) {
  if (total > 1_000_000) return `${(total / 1_000_000).toFixed(1)} t`;
  return `${Math.round(total)} kg`;
}

export default function KpiBar({ kpis }) {
  const cards = [
    ["Trains", kpis.trains],
    ["Gares", kpis.stations],
    ["Operateurs", kpis.operators],
    ["Dessertes", kpis.schedules],
    ["CO2 economise", formatCo2(kpis.co2)],
  ];

  return (
    <section className="kpi-grid">
      {cards.map(([label, value]) => (
        <article className="kpi" key={label}>
          <span>{label}</span>
          <strong>{value}</strong>
        </article>
      ))}
    </section>
  );
}
