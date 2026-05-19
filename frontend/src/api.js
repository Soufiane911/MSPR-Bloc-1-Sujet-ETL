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

  const endpoints = [
    { key: "summary",  path: "/stats/summary" },
    { key: "byCountry", path: "/stats/by-country" },
    { key: "dayNight", path: `/stats/day-night?country=${country && country !== "Tous" ? country : ""}` },
    { key: "routes",   path: "/stats/top-routes?limit=20" },
    { key: "quality",  path: "/stats/data-quality" },
    { key: "trains",   path: `/trains/?limit=1000${countryQuery}${typeQuery}` },
    { key: "stations", path: `/stations/?limit=1000${countryQuery}` },
    { key: "operators",path: `/operators/?limit=1000${countryQuery}` },
    { key: "schedules",path: `/schedules/?limit=1000${countryQuery}${typeQuery}` },
    { key: "trajets",  path: `/trajets/?limit=1000${countryQuery}${typeQuery}` },
  ];

  const results = await Promise.allSettled(
    endpoints.map((ep) => getJson(ep.path))
  );

  const data = {};
  const errors = [];

  results.forEach((result, i) => {
    const key = endpoints[i].key;
    if (result.status === "fulfilled") {
      data[key] = result.value;
    } else {
      data[key] = null;
      errors.push(`${endpoints[i].path} — ${result.reason.message}`);
    }
  });

  return { data, errors };
}
