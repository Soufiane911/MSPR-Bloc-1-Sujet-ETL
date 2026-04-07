import { Bar, BarChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export default function NetworkTab({ routes }) {
  const data = (routes || []).slice(0, 10).map((r) => ({
    route: `${r.origin_city || "?"} -> ${r.destination_city || "?"}`,
    frequency: r.frequency || 0,
    distance: r.avg_distance || 0,
  }));

  return (
    <section className="grid-2">
      <article className="card">
        <h3>Routes les plus frequentes</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={data}>
            <XAxis dataKey="route" hide />
            <YAxis />
            <Tooltip />
            <Bar dataKey="frequency" fill="#4b5eb0" />
          </BarChart>
        </ResponsiveContainer>
      </article>

      <article className="card">
        <h3>Distance moyenne des routes</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={data}>
            <XAxis dataKey="route" hide />
            <YAxis />
            <Tooltip />
            <Bar dataKey="distance" fill="#3d8f7e" />
          </BarChart>
        </ResponsiveContainer>
      </article>

      <article className="card full">
        <h3>Table routes</h3>
        <table className="table">
          <thead>
            <tr>
              <th>Origine</th>
              <th>Destination</th>
              <th>Frequence</th>
              <th>Duree moy.</th>
              <th>Distance moy.</th>
            </tr>
          </thead>
          <tbody>
            {(routes || []).slice(0, 12).map((r, i) => (
              <tr key={`${r.origin}-${r.destination}-${i}`}>
                <td>{r.origin_city || r.origin}</td>
                <td>{r.destination_city || r.destination}</td>
                <td>{r.frequency || 0}</td>
                <td>{r.avg_duration || 0} min</td>
                <td>{Math.round(r.avg_distance || 0)} km</td>
              </tr>
            ))}
          </tbody>
        </table>
      </article>
    </section>
  );
}
