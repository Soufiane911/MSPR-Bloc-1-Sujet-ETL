const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function getJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`API ${res.status}: ${path}`);
  return res.json();
}

export async function loadDashboardData(country, trainType) {
  const countryQuery = country && country !== "Tous" ? `&country=${country}` : "";
  const typeApi = trainType === "Jour" ? "day" : trainType === "Nuit" ? "night" : "";
  const typeQuery = typeApi ? `&train_type=${typeApi}` : "";

  const [summary, byCountry, dayNight, routes, quality, trains, stations, operators, schedules] =
    await Promise.all([
      getJson("/stats/summary"),
      getJson("/stats/by-country"),
      getJson(`/stats/day-night?country=${country && country !== "Tous" ? country : ""}`),
      getJson("/stats/top-routes?limit=20"),
      getJson("/stats/data-quality"),
      getJson(`/trains/?limit=1000${countryQuery}${typeQuery}`),
      getJson(`/stations/?limit=1000${countryQuery}`),
      getJson(`/operators/?limit=1000${countryQuery}`),
      getJson(`/schedules/?limit=1000${countryQuery}${typeQuery}`),
    ]);

  return { summary, byCountry, dayNight, routes, quality, trains, stations, operators, schedules };
}
