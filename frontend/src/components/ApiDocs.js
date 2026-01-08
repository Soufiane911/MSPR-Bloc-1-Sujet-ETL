export default function ApiDocs({ apiUrl }) {
  const endpoints = [
    {
      category: "Health & Database",
      items: [
        {
          method: "GET",
          path: "/health",
          description: "Check API and database connectivity",
          response: `{
  "status": "healthy",
  "database": "ok"
}`
        },
        {
          method: "POST",
          path: "/initdb",
          description: "Initialize database schema (requires ALLOW_INITDB=true)",
          response: `{
  "status": "database initialized"
}`
        },
        {
          method: "POST",
          path: "/import/seed",
          description: "Load development seed data",
          response: `{
  "status": "seed data imported",
  "agencies": 2,
  "trips": 3
}`
        },
        {
          method: "GET",
          path: "/export/csv",
          description: "Export entire database as ZIP of CSV files",
          response: `Returns a ZIP file containing:
- agencies.csv
- routes.csv
- stops.csv
- trips.csv
- stop_times.csv
- statistics.csv`
        },
        {
          method: "GET",
          path: "/export/csv/flat",
          description: "Export denormalized data as single CSV for PowerBI/Excel analysis",
          response: `Returns a single CSV file with columns:
tripId, headsign, direction, routeName, routeType,
agencyName, country, stopSequence, stopName,
stopLat, stopLon, arrivalTime, departureTime,
tripDuration, distance, avgSpeed, isNight`
        },
        {
          method: "GET",
          path: "/data/head",
          description: "Get sample rows (first 5-10) from all tables with total counts",
          response: `{
  "counts": {
    "agencies": 2,
    "routes": 15,
    "stops": 234,
    "trips": 1200,
    "stop_times": 15000,
    "statistics": 1200
  },
  "agencies": [...],
  "routes": [...],
  "stops": [...],
  "trips": [...],
  "stop_times": [...],
  "statistics": [...]
}`
        }
      ]
    },
    {
      category: "GTFS Import",
      items: [
        {
          method: "GET",
          path: "/import/gtfs/preflight",
          description: "Inspect GTFS feed before importing",
          params: "?url=<GTFS_ZIP_URL>",
          response: `{
  "status": "gtfs_preflight",
  "summary": {
    "agencies": { "present": 1, "rows": 2 },
    "routes": { "present": 1, "rows": 15 },
    "stops": { "present": 1, "rows": 234 },
    "trips": { "present": 1, "rows": 1200 },
    "stopTimes": { "present": 1, "rows": 15000 },
    "hasCoreSchedules": 1,
    "hasCalendar": 1
  }
}`
        },
        {
          method: "POST",
          path: "/import/gtfs",
          description: "Import GTFS feed from URL or file upload",
          params: "?url=<URL> OR multipart/form-data file",
          response: `{
  "status": "gtfs_imported",
  "preflight": { "hasCoreSchedules": 1 },
  "stats": {
    "agencies": 2,
    "routes": 15,
    "stops": 234,
    "trips": 1200,
    "stopTimes": 15000
  }
}`
        }
      ]
    },
    {
      category: "Trips (Trajets)",
      items: [
        {
          method: "GET",
          path: "/trajets",
          description: "List trips with filters and pagination",
          params: "?limit=10&offset=0&agency_id=1&is_night=false",
          response: `{
  "total": 1200,
  "limit": 10,
  "offset": 0,
  "trajets": [{
    "id": 1,
    "ligne": {
      "nom_long": "Paris - Luxembourg",
      "nom_court": "TGV",
      "type": "rail"
    },
    "agence": { "nom": "SNCF" },
    "destination": "Luxembourg",
    "train_de_nuit": false,
    "horaires": [{
      "sequence": 1,
      "arret": "Paris Est",
      "departure": "08:00"
    }],
    "stats": {
      "distance_km": 372.5,
      "duree_minutes": 135,
      "co2_total_g": 5587.5
    }
  }]
}`
        },
        {
          method: "GET",
          path: "/trajets/<id>",
          description: "Get single trip details by ID",
          response: `{
  "id": 1,
  "ligne": { "nom_long": "Paris - Luxembourg" },
  "agence": {
    "nom": "SNCF",
    "url": "https://sncf.com",
    "fuseau_horaire": "Europe/Paris"
  },
  "destination": "Luxembourg",
  "horaires": [{
    "arret": {
      "nom": "Paris Est",
      "latitude": 48.8768,
      "longitude": 2.3590
    },
    "departure": "08:00"
  }],
  "stats": {
    "distance_km": 372.5,
    "vitesse_moyenne_kmh": 165.3
  }
}`
        }
      ]
    },
    {
      category: "Statistics",
      items: [
        {
          method: "GET",
          path: "/stats/volumes",
          description: "Aggregate trip statistics by agency",
          params: "?agency_id=1 (optional)",
          response: `{
  "summary": {
    "total_agencies": 2,
    "total_trips": 1200
  },
  "by_agency": [{
    "agency_id": 1,
    "agency_name": "SNCF",
    "day_trips": {
      "count": 800,
      "total_distance_km": 298000.0,
      "avg_co2_per_passenger_g": 55.9
    },
    "night_trips": {
      "count": 100
    }
  }]
}`
        }
      ]
    }
  ];

  return (
    <div className="api-docs-container">
      <div className="api-docs-header">
        <h2>API Documentation</h2>
        <div className="api-base-url">
          <span className="label">Base URL:</span>
          <code>{apiUrl}</code>
        </div>
      </div>

      {endpoints.map((section, idx) => (
        <div key={idx} className="api-section">
          <h3 className="api-category">{section.category}</h3>
          {section.items.map((endpoint, eidx) => (
            <div key={eidx} className="api-endpoint">
              <div className="endpoint-header">
                <span className={`method method-${endpoint.method.toLowerCase()}`}>
                  {endpoint.method}
                </span>
                <code className="endpoint-path">{endpoint.path}</code>
              </div>
              <p className="endpoint-description">{endpoint.description}</p>
              {endpoint.params && (
                <div className="endpoint-params">
                  <strong>Params:</strong> <code>{endpoint.params}</code>
                </div>
              )}
              <div className="endpoint-response">
                <strong>Response:</strong>
                <pre><code>{endpoint.response}</code></pre>
              </div>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}
