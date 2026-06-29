function getDefaultApiBase() {
  if (typeof window === "undefined") {
    return "http://localhost:8000";
  }
  return `${window.location.protocol}//${window.location.hostname}:8000`;
}

const API_BASE = import.meta.env.VITE_API_BASE_URL || getDefaultApiBase();

async function getJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`API ${res.status}: ${path}`);
  return res.json();
}

export async function loadDashboardData(country, trainType) {
  const countryValue = country && country !== "Tous" ? encodeURIComponent(country) : "";
  const countryQuery = countryValue ? `&country=${countryValue}` : "";
  const typeApi = trainType === "Jour" ? "day" : trainType === "Nuit" ? "night" : "";
  const typeQuery = typeApi ? `&train_type=${encodeURIComponent(typeApi)}` : "";
  const dayNightPath = countryValue ? `/stats/day-night?country=${countryValue}` : "/stats/day-night";

  const [summary, byCountry, dayNight, routes, quality, trains, stations, operators, schedules, trajets] =
    await Promise.all([
      getJson("/stats/summary"),
      getJson("/stats/by-country"),
      getJson(dayNightPath),
      getJson("/stats/top-routes?limit=20"),
      getJson("/stats/data-quality"),
      getJson(`/trains/?limit=1000${countryQuery}${typeQuery}`),
      getJson(`/stations/?limit=1000${countryQuery}`),
      getJson(`/operators/?limit=1000${countryQuery}`),
      getJson(`/schedules/?limit=1000${countryQuery}${typeQuery}`),
      getJson(`/trajets?limit=1000${countryQuery}${typeQuery}`),
    ]);

  return { summary, byCountry, dayNight, routes, quality, trains, stations, operators, schedules, trajets };
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

export async function loadTrajets({ country, trainType, limit = 200, offset = 0 } = {}) {
  const params = new URLSearchParams();
  if (country) params.set("country", country);
  if (trainType) params.set("train_type", trainType);
  params.set("limit", String(limit));
  params.set("offset", String(offset));
  return getJson(`/trajets?${params.toString()}`);
}

export async function predictRoute(payload) {
  const res = await fetch(`${API_BASE}/predict/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`API ${res.status}: /predict/`);
  return res.json();
}

export async function loadFlightChoices(limit = 20) {
  try {
    return await getJson(`/aviationStats/topRoutes?limit=${Math.max(1, Math.min(50, limit))}`);
  } catch {
    // Fallback static routes if aviation endpoints are not available on this branch.
    return [
      {
        id: "CDG-MXP",
        origin_iata: "CDG",
        destination_iata: "MXP",
        origin_name: "Paris",
        destination_name: "Milan",
        origin_country: "FR",
        destination_country: "IT",
        avg_distance_km: 640,
      },
      {
        id: "PAR-BER",
        origin_iata: "ORY",
        destination_iata: "BER",
        origin_name: "Paris",
        destination_name: "Berlin",
        origin_country: "FR",
        destination_country: "DE",
        avg_distance_km: 880,
      },
      {
        id: "MAD-BCN",
        origin_iata: "MAD",
        destination_iata: "BCN",
        origin_name: "Madrid",
        destination_name: "Barcelona",
        origin_country: "ES",
        destination_country: "ES",
        avg_distance_km: 505,
      },
    ];
  }
}
