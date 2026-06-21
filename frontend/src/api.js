const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

async function getJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`API ${res.status}: ${path}`);
  return res.json();
}

async function postJson(path, payload) {
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: payload ? JSON.stringify(payload) : undefined,
  });
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

export async function loadWatchOverview() {
  return getJson("/watch/");
}

export async function loadWatchItems({ source, tag, limit = 50, offset = 0 } = {}) {
  const params = new URLSearchParams();
  if (source && source !== "all") params.set("source", source);
  if (tag) params.set("tag", tag);
  params.set("limit", String(limit));
  params.set("offset", String(offset));
  return getJson(`/watch/items?${params.toString()}`);
}

export async function loadWatchSources() {
  return getJson("/watch/sources");
}

export async function refreshWatch() {
  return postJson("/watch/refresh");
}
