import React, { useEffect, useMemo, useState } from "react";
import { loadFlightChoices, loadTrajets, predictRoute } from "../api";

const CO2_FLIGHT_PER_KM = 0.255;
const CO2_TRAIN_PER_KM = 0.041;

function normalizeFlightRow(row, idx) {
  const originCountry = row.origin_country || row.originCountry || "EU";
  const destinationCountry = row.destination_country || row.destinationCountry || originCountry;
  const distanceKm = Number(row.avg_distance_km || row.distance_km || 0);
  return {
    id: row.id || `${row.origin_iata || row.origin_name}-${row.destination_iata || row.destination_name}-${idx}`,
    label: `${row.origin_iata || row.origin_name || "?"} -> ${row.destination_iata || row.destination_name || "?"}`,
    originCountry,
    destinationCountry,
    distanceKm,
  };
}

function pickTrainAlternative(trajets, flight) {
  if (!trajets?.length) return null;

  const targetDistance = flight.distanceKm || 0;
  const ranked = [...trajets].sort((a, b) => {
    const da = Math.abs(Number(a.distance_km || 0) - targetDistance);
    const db = Math.abs(Number(b.distance_km || 0) - targetDistance);
    return da - db;
  });

  return ranked[0] || null;
}

function toPredictPayload(train, flight) {
  const distanceKm = Number(train.distance_km || flight.distanceKm || 0);
  const durationMin = Number(train.duration_min || 1);
  const isInternational = (train.origin_country || flight.originCountry) !== (train.destination_country || flight.destinationCountry);
  const trainType = train.train_type === "night" ? "night" : "day";
  const weeklyFrequency = 7;
  const estimatedSaving = Math.max(0, distanceKm * (CO2_FLIGHT_PER_KM - CO2_TRAIN_PER_KM));

  return {
    distance_km: Math.max(distanceKm, 1),
    duration_min: Math.max(durationMin, 1),
    is_international: isInternational,
    train_type: trainType,
    weekly_frequency: weeklyFrequency,
    estimated_co2_saving_kg: Number(estimatedSaving.toFixed(2)),
    origin_country: train.origin_country || flight.originCountry || "EU",
    destination_country: train.destination_country || flight.destinationCountry || "EU",
  };
}

export default function DemoPredictPage() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [flights, setFlights] = useState([]);
  const [selectedFlightId, setSelectedFlightId] = useState("");
  const [matchingTrain, setMatchingTrain] = useState(null);
  const [prediction, setPrediction] = useState(null);

  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError("");

    loadFlightChoices(20)
      .then((rows) => {
        if (!mounted) return;
        const list = rows.map(normalizeFlightRow).filter((r) => r.distanceKm > 0);
        setFlights(list);
        setSelectedFlightId(list[0]?.id || "");
      })
      .catch((err) => mounted && setError(err.message || "Impossible de charger les trajets avion"))
      .finally(() => mounted && setLoading(false));

    return () => {
      mounted = false;
    };
  }, []);

  const selectedFlight = useMemo(
    () => flights.find((f) => f.id === selectedFlightId) || null,
    [flights, selectedFlightId]
  );

  const runPrediction = async () => {
    if (!selectedFlight) return;
    setError("");
    setPrediction(null);
    setMatchingTrain(null);

    try {
      const [originTrajets, destinationTrajets, broadTrajets] = await Promise.all([
        loadTrajets({ country: selectedFlight.originCountry, limit: 300 }),
        selectedFlight.destinationCountry !== selectedFlight.originCountry
          ? loadTrajets({ country: selectedFlight.destinationCountry, limit: 300 })
          : Promise.resolve([]),
        loadTrajets({ limit: 300 }),
      ]);

      const candidates = [...originTrajets, ...destinationTrajets, ...broadTrajets];
      let train = pickTrainAlternative(candidates, selectedFlight);

      // Fallback synthétique pour garantir une démo fonctionnelle même sans matching DB.
      if (!train) {
        const estDistance = selectedFlight.distanceKm * 1.08;
        const estDuration = Math.round((estDistance / 120) * 60);
        train = {
          trajet_id: -1,
          train_number: "DEMO-ALT",
          operator_name: "ObRail Synthétique",
          train_type: estDuration > 420 ? "night" : "day",
          origin: selectedFlight.label.split(" -> ")[0],
          destination: selectedFlight.label.split(" -> ")[1],
          origin_country: selectedFlight.originCountry,
          destination_country: selectedFlight.destinationCountry,
          distance_km: estDistance,
          duration_min: estDuration,
        };
      }

      setMatchingTrain(train);
      const payload = toPredictPayload(train, selectedFlight);
      const result = await predictRoute(payload);
      setPrediction({ payload, result });
    } catch (err) {
      setError(err.message || "Erreur lors de la prédiction");
    }
  };

  const co2Saving = prediction?.payload?.estimated_co2_saving_kg ?? 0;

  return (
    <main className="demo-shell">
      <section className="content">
        <header>
          <h1>Démo IA — Substitution Avion vers Train</h1>
          <p>Sélectionnez un trajet avion, visualisez une alternative train, puis exécutez la prédiction ML.</p>
          <p>
            <a href="/">← Dashboard ferroviaire</a>
            {" | "}
            <a href="/aviation">Aviation</a>
          </p>
        </header>

        {loading && <div className="card">Chargement des trajets avion...</div>}
        {error && <div className="card error">Erreur: {error}</div>}

        {!loading && (
          <>
            <section className="card demo-controls">
              <label>
                Trajet avion
                <select value={selectedFlightId} onChange={(e) => setSelectedFlightId(e.target.value)}>
                  {flights.map((flight) => (
                    <option key={flight.id} value={flight.id}>
                      {flight.label} ({Math.round(flight.distanceKm)} km)
                    </option>
                  ))}
                </select>
              </label>
              <button type="button" onClick={runPrediction} disabled={!selectedFlight}>
                Lancer la prédiction
              </button>
            </section>

            {selectedFlight && (
              <section className="grid-2">
                <article className="card">
                  <h3>Trajet avion choisi</h3>
                  <p><strong>Liaison:</strong> {selectedFlight.label}</p>
                  <p><strong>Distance estimée:</strong> {Math.round(selectedFlight.distanceKm)} km</p>
                  <p><strong>Pays:</strong> {selectedFlight.originCountry}{" -> "}{selectedFlight.destinationCountry}</p>
                </article>

                <article className="card">
                  <h3>Alternative train proposée</h3>
                  {!matchingTrain && <p className="muted">Lancez la prédiction pour rechercher un trajet train.</p>}
                  {matchingTrain && (
                    <>
                      <p><strong>Train:</strong> {matchingTrain.train_number} ({matchingTrain.operator_name})</p>
                      <p><strong>Type:</strong> {matchingTrain.train_type}</p>
                      <p><strong>Route:</strong> {matchingTrain.origin}{" -> "}{matchingTrain.destination}</p>
                      <p><strong>Distance:</strong> {Math.round(Number(matchingTrain.distance_km || 0))} km</p>
                      <p><strong>Durée:</strong> {matchingTrain.duration_min || "n/a"} min</p>
                    </>
                  )}
                </article>
              </section>
            )}

            {prediction && (
              <section className="card demo-result">
                <h3>Résultat du modèle</h3>
                <div className="kpi-grid demo-kpi-grid">
                  <div className="kpi">
                    <span>Classe prédite</span>
                    <strong>{prediction.result.prediction}</strong>
                  </div>
                  <div className="kpi">
                    <span>Confiance</span>
                    <strong>{Math.round((prediction.result.confidence || 0) * 100)}%</strong>
                  </div>
                  <div className="kpi">
                    <span>Gain CO2 estimé</span>
                    <strong>{co2Saving.toFixed(1)} kg</strong>
                  </div>
                  <div className="kpi">
                    <span>Version modèle</span>
                    <strong>{prediction.result.model_version}</strong>
                  </div>
                </div>

                <p><strong>Explication:</strong> {prediction.result.explanation}</p>

                <h4>Probabilités par classe</h4>
                <table className="table">
                  <thead>
                    <tr>
                      <th>Classe</th>
                      <th>Probabilité</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(prediction.result.probabilities || {}).map(([label, value]) => (
                      <tr key={label}>
                        <td>{label}</td>
                        <td>{Math.round(Number(value) * 100)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </section>
            )}
          </>
        )}
      </section>
    </main>
  );
}
