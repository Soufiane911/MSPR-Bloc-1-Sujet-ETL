import { Bar, BarChart, Cell, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export default function OverviewTab({ trains, byCountry }) {
  const day = trains.filter((t) => t.train_type === "day").length;
  const night = trains.filter((t) => t.train_type === "night").length;
  const pieData = [
    { name: "Jour", value: day, color: "#f8ca3f" },
    { name: "Nuit", value: night, color: "#20335f" },
  ];

  const countryData = (byCountry || []).slice(0, 10).map((r) => ({
    country: r.country || "N/A",
    trains: r.nb_trains || 0,
  }));

  return (
    <section className="grid-2">
      <article className="card">
        <h3>Repartition Jour/Nuit</h3>
        <ResponsiveContainer width="100%" height={300}>
          <PieChart>
            <Pie data={pieData} dataKey="value" nameKey="name" outerRadius={110}>
              {pieData.map((entry) => (
                <Cell key={entry.name} fill={entry.color} />
              ))}
            </Pie>
            <Tooltip />
          </PieChart>
        </ResponsiveContainer>
      </article>

      <article className="card">
        <h3>Top pays</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={countryData}>
            <XAxis dataKey="country" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="trains" fill="#2f7f85" />
          </BarChart>
        </ResponsiveContainer>
      </article>
    </section>
  );
}
