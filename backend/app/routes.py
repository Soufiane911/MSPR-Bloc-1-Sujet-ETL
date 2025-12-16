import time
from flask import Blueprint, jsonify, current_app, request
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func, text
from prometheus_client import Counter, Summary
from .db import db
from .seeds import seedDevData
from .gtfs_importer import GTFSImporter
from .models import Trajet, Arret, HorairePassage, Agence, Ligne, StatistiquesTrajet

api = Blueprint("api", __name__)

gtfsImportCounter = Counter("gtfs_import_total", "GTFS import attempts", ["result"])
gtfsImportDuration = Summary("gtfs_import_seconds", "GTFS import duration seconds", ["result"])


@api.get("/test")
def test():
    return jsonify({"message": "API is working"})


@api.post("/initdb")
def initDbRoute():
    if not current_app.config.get("ALLOW_INITDB", False):
        return jsonify({"error": "forbidden"}), 403

    try:
        db.create_all()
        return jsonify({"status": "databaseInitialized"})
    except SQLAlchemyError as exc:
        current_app.logger.exception("DB init failed")
        return jsonify({"error": "dbInitFailed", "details": str(exc)}), 500


@api.post("/import/seed")
def importSeed():
    if not current_app.config.get("ALLOW_INITDB", False):
        return jsonify({"error": "forbidden"}), 403

    try:
        seedDevData()
        return jsonify({"status": "seedDataImported", "agencies": 2, "trips": 3})
    except Exception as exc:
        current_app.logger.exception("Seed import failed")
        return jsonify({"error": "seedImportFailed", "details": str(exc)}), 500


@api.post("/import/gtfs")
def importGtfs():
    if not current_app.config.get("ALLOW_INITDB", False):
        return jsonify({"error": "forbidden"}), 403

    startTime = time.perf_counter()
    resultLabel = "failure"
    gtfsUrl = request.args.get("url")
    gtfsFile = request.files.get("file")

    try:
        if not gtfsUrl and not gtfsFile:
            return jsonify({"error": "missingInput", "details": "Provide either ?url=<url> query param or upload file"}), 400

        summary = None

        if gtfsFile:
            import io, zipfile

            fileBytes = gtfsFile.read()
            importer = GTFSImporter(None)
            try:
                importer.zipFile = zipfile.ZipFile(io.BytesIO(fileBytes))
            except zipfile.BadZipFile as exc:
                return jsonify({"error": "invalidZip", "details": str(exc)}), 400
            summary = importer.inspectFeed()
            success = importer.parseAndImport()
        else:
            importer = GTFSImporter(gtfsUrl)
            if not importer.downloadAndParse():
                return jsonify({"error": "downloadFailed", "details": importer.errors}), 400
            summary = importer.inspectFeed()
            success = importer.parseAndImport()

        if success:
            resultLabel = "success"
            note = None
            if summary and not summary.get("hasCoreSchedules"):
                note = "Feed contains no trips/stop_times. Routes and stops are imported; trips will be empty. Use /import/gtfs/preflight to verify feeds before import."
            return jsonify({"status": "gtfsImported", "preflight": summary, "stats": importer.stats, "note": note, "errors": importer.errors}), 200
        return jsonify({"error": "gtfsImportFailed", "details": importer.errors, "preflight": summary}), 500

    finally:
        elapsed = time.perf_counter() - startTime
        gtfsImportDuration.labels(resultLabel).observe(elapsed)
        gtfsImportCounter.labels(resultLabel).inc()


@api.get("/import/gtfs/preflight")
def importGtfsPreflight():
    if not current_app.config.get("ALLOW_INITDB", False):
        return jsonify({"error": "forbidden"}), 403

    gtfsUrl = request.args.get("url")
    if not gtfsUrl:
        return jsonify({"error": "missingUrl", "details": "?url=https://..."}), 400

    importer = GTFSImporter(gtfsUrl)
    if not importer.downloadAndParse():
        return jsonify({"error": "downloadFailed", "details": importer.errors}), 400

    summary = importer.inspectFeed()
    return jsonify({"status": "gtfsPreflight", "summary": summary}), 200


@api.get("/trajets")
def listTrajets():
    try:
        query = db.session.query(Trajet).join(Ligne).join(Agence)

        agencyId = request.args.get("agency_id", type=int)
        if agencyId:
            query = query.filter(Ligne.idAgence == agencyId)

        isNight = request.args.get("is_night")
        if isNight is not None:
            isNightBool = isNight.lower() == "true"
            query = query.filter(Trajet.trainDeNuit == isNightBool)

        limitValue = request.args.get("limit", default=50, type=int)
        offsetValue = request.args.get("offset", default=0, type=int)
        limitValue = min(limitValue, 200)

        total = query.count()
        trajets = query.offset(offsetValue).limit(limitValue).all()

        result = {
            "total": total,
            "limit": limitValue,
            "offset": offsetValue,
            "trajets": [
                {
                    "id": trajet.idTrajet,
                    "ligne": {
                        "id": trajet.ligne.idLigne,
                        "nomLong": trajet.ligne.nomLong,
                        "nomCourt": trajet.ligne.nomCourt,
                        "type": trajet.ligne.typeLigne,
                    },
                    "agence": {
                        "id": trajet.ligne.agence.idAgence,
                        "nom": trajet.ligne.agence.nomAgence,
                    },
                    "destination": trajet.destination,
                    "trainDeNuit": trajet.trainDeNuit,
                    "horaires": [
                        {
                            "sequence": horaire.sequenceArret,
                            "arret": horaire.arret.nomArret,
                            "arrival": horaire.heureArrivee.strftime("%H:%M") if horaire.heureArrivee else None,
                            "departure": horaire.heureDepart.strftime("%H:%M") if horaire.heureDepart else None,
                        }
                        for horaire in sorted(trajet.horaires, key=lambda horaire_item: horaire_item.sequenceArret)
                    ],
                    "stats": {
                        "distanceKm": float(trajet.stats.distanceKm) if trajet.stats and trajet.stats.distanceKm else None,
                        "dureeMinutes": trajet.stats.dureeMinutes if trajet.stats else None,
                        "co2TotalG": float(trajet.stats.co2TotalG) if trajet.stats and trajet.stats.co2TotalG else None,
                        "co2ParPassagerG": float(trajet.stats.co2ParPassagerG) if trajet.stats and trajet.stats.co2ParPassagerG else None,
                    }
                    if trajet.stats
                    else None,
                }
                for trajet in trajets
            ],
        }

        return jsonify(result), 200

    except Exception as exc:
        current_app.logger.exception("Error listing trajets")
        return jsonify({"error": "listTrajetsFailed", "details": str(exc)}), 500


@api.get("/trajets/<int:trajetId>")
def getTrajet(trajetId):
    try:
        trajet = db.session.query(Trajet).filter(Trajet.idTrajet == trajetId).first()
        if not trajet:
            return jsonify({"error": "notFound"}), 404

        result = {
            "id": trajet.idTrajet,
            "ligne": {
                "id": trajet.ligne.idLigne,
                "nomLong": trajet.ligne.nomLong,
                "nomCourt": trajet.ligne.nomCourt,
                "type": trajet.ligne.typeLigne,
            },
            "agence": {
                "id": trajet.ligne.agence.idAgence,
                "nom": trajet.ligne.agence.nomAgence,
                "url": trajet.ligne.agence.url,
                "fuseauHoraire": trajet.ligne.agence.fuseauHoraire,
            },
            "destination": trajet.destination,
            "trainDeNuit": trajet.trainDeNuit,
            "horaires": [
                {
                    "sequence": horaire.sequenceArret,
                    "arret": {
                        "id": horaire.arret.idArret,
                        "nom": horaire.arret.nomArret,
                        "latitude": float(horaire.arret.latitude),
                        "longitude": float(horaire.arret.longitude),
                    },
                    "arrival": horaire.heureArrivee.strftime("%H:%M") if horaire.heureArrivee else None,
                    "departure": horaire.heureDepart.strftime("%H:%M") if horaire.heureDepart else None,
                }
                for horaire in sorted(trajet.horaires, key=lambda horaire_item: horaire_item.sequenceArret)
            ],
            "stats": {
                "distanceKm": float(trajet.stats.distanceKm) if trajet.stats and trajet.stats.distanceKm else None,
                "dureeMinutes": trajet.stats.dureeMinutes if trajet.stats else None,
                "vitesseMoyenneKmh": float(trajet.stats.vitesseMoyenneKmh) if trajet.stats and trajet.stats.vitesseMoyenneKmh else None,
                "co2TotalG": float(trajet.stats.co2TotalG) if trajet.stats and trajet.stats.co2TotalG else None,
                "co2ParPassagerG": float(trajet.stats.co2ParPassagerG) if trajet.stats and trajet.stats.co2ParPassagerG else None,
            }
            if trajet.stats
            else None,
        }

        return jsonify(result), 200

    except Exception as exc:
        current_app.logger.exception(f"Error fetching trajet {trajetId}")
        return jsonify({"error": "getTrajetFailed", "details": str(exc)}), 500


@api.get("/stats/volumes")
def statsVolumes():
    try:
        from sqlalchemy import cast, Float

        query = db.session.query(
            Agence.idAgence,
            Agence.nomAgence,
            Trajet.trainDeNuit,
            func.count(Trajet.idTrajet).label("tripCount"),
            func.coalesce(func.sum(cast(StatistiquesTrajet.distanceKm, Float)), 0).label("totalDistanceKm"),
            func.coalesce(func.avg(cast(StatistiquesTrajet.distanceKm, Float)), 0).label("avgDistanceKm"),
            func.coalesce(func.sum(cast(StatistiquesTrajet.co2TotalG, Float)), 0).label("totalCo2G"),
            func.coalesce(func.avg(cast(StatistiquesTrajet.co2ParPassagerG, Float)), 0).label("avgCo2PerPaxG"),
        ).select_from(Trajet).join(
            Ligne, Trajet.idLigne == Ligne.idLigne
        ).join(
            Agence, Ligne.idAgence == Agence.idAgence
        ).outerjoin(
            StatistiquesTrajet, StatistiquesTrajet.idTrajet == Trajet.idTrajet
        ).group_by(
            Agence.idAgence, Agence.nomAgence, Trajet.trainDeNuit
        )

        agencyId = request.args.get("agency_id", type=int)
        if agencyId:
            query = query.filter(Agence.idAgence == agencyId)

        results = query.all()

        stats = {
            "summary": {"totalAgencies": len(set(result[0] for result in results)), "totalTrips": sum(result[3] for result in results)},
            "byAgency": [],
        }

        byAgency = {}
        for row in results:
            currentAgencyId, agencyName, isNight, tripCount, totalDist, avgDist, totalCo2, avgCo2 = row
            key = f"{currentAgencyId}:{agencyName}"
            if key not in byAgency:
                byAgency[key] = {"agencyId": currentAgencyId, "agencyName": agencyName, "dayTrips": None, "nightTrips": None}
            if isNight:
                byAgency[key]["nightTrips"] = {
                    "count": int(tripCount),
                    "totalDistanceKm": float(totalDist) if totalDist else 0,
                    "avgDistanceKm": float(avgDist) if avgDist else 0,
                    "totalCo2G": float(totalCo2) if totalCo2 else 0,
                    "avgCo2PerPassengerG": float(avgCo2) if avgCo2 else 0,
                }
            else:
                byAgency[key]["dayTrips"] = {
                    "count": int(tripCount),
                    "totalDistanceKm": float(totalDist) if totalDist else 0,
                    "avgDistanceKm": float(avgDist) if avgDist else 0,
                    "totalCo2G": float(totalCo2) if totalCo2 else 0,
                    "avgCo2PerPassengerG": float(avgCo2) if avgCo2 else 0,
                }

        stats["byAgency"] = list(byAgency.values())
        return jsonify(stats), 200

    except Exception as exc:
        current_app.logger.exception("Error computing stats/volumes")
        return jsonify({"error": "statsFailed", "details": str(exc)}), 500


@api.get("/health")
def health():
    try:
        db.session.execute(text("SELECT 1"))
        return jsonify({"status": "healthy", "database": "ok"}), 200
    except Exception as exc:
        current_app.logger.error(f"Health check failed: {exc}")
        return jsonify({"status": "unhealthy", "database": "error", "details": str(exc)}), 503
