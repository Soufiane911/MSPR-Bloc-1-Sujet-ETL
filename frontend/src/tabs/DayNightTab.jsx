import { Bar, BarChart, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export default function DayNightTab({ dayNight }) {
  const data = (dayNight || []).map((row) => ({
    country: row.country,
    type: row.train_type,
    trains: row.nb_trains,
    schedules: row.nb_schedules,
  }));

  return (
    <section className="grid-2">
      <article className="card">
        <h3>Nombre de trains par pays</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={data}>
            <XAxis dataKey="country" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="trains" fill="#335f9f" name="Trains" />
          </BarChart>
        </ResponsiveContainer>
      </article>

      <article className="card">
        <h3>Dessertes par pays</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={data}>
            <XAxis dataKey="country" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="schedules" fill="#20335f" name="Dessertes" />
          </BarChart>
        </ResponsiveContainer>
      </article>
    </section>
  );
}
