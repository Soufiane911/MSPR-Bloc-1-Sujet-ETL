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
      getJson("/stats/byCountry"),
      getJson(`/stats/dayNight?country=${country && country !== "Tous" ? country : ""}`),
      getJson("/stats/topRoutes?limit=20"),
      getJson("/stats/dataQuality"),
      getJson(`/trains/?limit=1000${countryQuery}${typeQuery}`),
      getJson(`/stations/?limit=1000${countryQuery}`),
      getJson(`/operators/?limit=1000${countryQuery}`),
      getJson(`/schedules/?limit=1000${countryQuery}${typeQuery}`),
    ]);

  return { summary, byCountry, dayNight, routes, quality, trains, stations, operators, schedules };
}

export async function loadAviationData(limit = 50) {
  const [summary, byCountry, topRoutes, topAirports, airlineSummary, dataQuality] = await Promise.all([
    getJson('/aviationStats/summary'),
    getJson('/aviationStats/byCountry'),
    getJson(`/aviationStats/topRoutes?limit=${Math.max(1, Math.min(50, limit))}`),
    getJson(`/aviationStats/topAirports?limit=${Math.max(1, Math.min(50, limit))}`),
    getJson('/aviationStats/airlineSummary'),
    getJson('/aviationStats/dataQuality'),
  ]);

  return { summary, byCountry, topRoutes, topAirports, airlineSummary, dataQuality };
}
