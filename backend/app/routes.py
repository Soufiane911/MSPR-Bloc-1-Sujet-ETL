import time
import json
import io
import csv
import zipfile
from flask import Blueprint, jsonify, current_app, request, Response, stream_with_context, send_file
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


@api.get("/import/gtfs/stream")
def importGtfsStream():
    if not current_app.config.get("ALLOW_INITDB", False):
        return jsonify({"error": "forbidden"}), 403

    gtfsUrl = request.args.get("url")
    if not gtfsUrl:
        return jsonify({"error": "missingUrl"}), 400

    def generate():
        def emitProgress(status, data):
            yield f"data: {json.dumps({'status': status, 'data': data})}\n\n"

        importer = GTFSImporter(gtfsUrl, progressCallback=emitProgress)
        
        try:
            if not importer.downloadAndParse():
                yield f"data: {json.dumps({'status': 'error', 'data': {'message': 'Download failed', 'errors': importer.errors}})}\n\n"
                return
        except Exception as exc:
            yield f"data: {json.dumps({'status': 'error', 'data': {'message': str(exc)}})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


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


@api.get("/stats/co2-by-type")
def statsCo2ByType():
    try:
        from sqlalchemy import cast, Float

        query = db.session.query(
            Ligne.typeLigne.label("type"),
            func.count(Trajet.idTrajet).label("tripCount"),
            func.coalesce(func.sum(cast(StatistiquesTrajet.co2TotalG, Float)), 0).label("totalCo2G"),
            func.coalesce(func.avg(cast(StatistiquesTrajet.co2ParPassagerG, Float)), 0).label("avgCo2PerPaxG"),
            func.coalesce(func.avg(cast(StatistiquesTrajet.distanceKm, Float)), 0).label("avgDistanceKm"),
        ).select_from(Trajet).join(
            Ligne, Trajet.idLigne == Ligne.idLigne
        ).outerjoin(
            StatistiquesTrajet, StatistiquesTrajet.idTrajet == Trajet.idTrajet
        ).group_by(
            Ligne.typeLigne
        )

        results = query.all()

        data = [
            {
                "type": row[0],
                "tripCount": int(row[1]),
                "totalCo2G": float(row[2]) if row[2] else 0,
                "avgCo2PerPassengerG": float(row[3]) if row[3] else 0,
                "avgDistanceKm": float(row[4]) if row[4] else 0,
            }
            for row in results
        ]

        summary = {
            "totalTrips": sum(item["tripCount"] for item in data),
            "totalCo2G": sum(item["totalCo2G"] for item in data),
        }

        return jsonify({"summary": summary, "byType": data}), 200

    except Exception as exc:
        current_app.logger.exception("Error computing stats/co2-by-type")
        return jsonify({"error": "statsFailed", "details": str(exc)}), 500


@api.get("/health")
def health():
    try:
        db.session.execute(text("SELECT 1"))
        return jsonify({"status": "healthy", "database": "ok"}), 200
    except Exception as exc:
        current_app.logger.error(f"Health check failed: {exc}")
        return jsonify({"status": "unhealthy", "database": "error", "details": str(exc)}), 503


@api.get("/data/head")
def dataHead():
    try:
        agencies = []
        for a in db.session.query(Agence).limit(5).all():
            agencies.append({
                "id": a.idAgence,
                "name": a.nomAgence,
                "url": a.url,
                "timezone": a.fuseauHoraire
            })
        
        routes = []
        for r in db.session.query(Ligne).limit(5).all():
            routes.append({
                "id": r.idLigne,
                "agency_id": r.idAgence,
                "short_name": r.nomCourt,
                "long_name": r.nomLong,
                "type": r.typeLigne
            })
        
        stops = []
        for s in db.session.query(Arret).limit(5).all():
            stops.append({
                "id": s.idArret,
                "name": s.nomArret,
                "lat": float(s.latitude) if s.latitude else None,
                "lon": float(s.longitude) if s.longitude else None,
                "zone": s.idZone
            })
        
        trips = []
        for t in db.session.query(Trajet).limit(5).all():
            trips.append({
                "id": t.idTrajet,
                "route_id": t.idLigne,
                "destination": t.destination,
                "night": t.trainDeNuit
            })
        
        stopTimes = []
        for h in db.session.query(HorairePassage).limit(10).all():
            stopTimes.append({
                "trip_id": h.idTrajet,
                "stop_id": h.idArret,
                "sequence": h.sequenceArret,
                "arrival": h.heureArrivee.strftime('%H:%M:%S') if h.heureArrivee else None,
                "departure": h.heureDepart.strftime('%H:%M:%S') if h.heureDepart else None
            })
        
        stats = []
        for s in db.session.query(StatistiquesTrajet).limit(5).all():
            stats.append({
                "trip_id": s.idTrajet,
                "distance_km": float(s.distanceKm) if s.distanceKm else None,
                "duration_min": s.dureeMinutes,
                "avg_speed_kmh": float(s.vitesseMoyenneKmh) if s.vitesseMoyenneKmh else None
            })
        
        counts = {
            "agencies": db.session.query(Agence).count(),
            "routes": db.session.query(Ligne).count(),
            "stops": db.session.query(Arret).count(),
            "trips": db.session.query(Trajet).count(),
            "stop_times": db.session.query(HorairePassage).count(),
            "statistics": db.session.query(StatistiquesTrajet).count()
        }
        
        return jsonify({
            "counts": counts,
            "agencies": agencies,
            "routes": routes,
            "stops": stops,
            "trips": trips,
            "stop_times": stopTimes,
            "statistics": stats
        })
    except Exception as exc:
        current_app.logger.exception("Data head failed")
        return jsonify({"error": "dataHeadFailed", "details": str(exc)}), 500


@api.get("/export/csv")
def exportCsv():
    try:
        memoryFile = io.BytesIO()
        
        with zipfile.ZipFile(memoryFile, 'w', zipfile.ZIP_DEFLATED) as zipf:
            tables = [
                ('agencies', Agence, ['id', 'nom', 'pays', 'url', 'timezone']),
                ('routes', Ligne, ['id', 'id_agence', 'nom_court', 'nom_long', 'type_transport', 'couleur', 'couleur_text']),
                ('stops', Arret, ['id', 'nom', 'latitude', 'longitude', 'type_arret']),
                ('trips', Trajet, ['id', 'id_ligne', 'headsign', 'direction_id']),
                ('statistics', StatistiquesTrajet, ['id', 'id_trajet', 'duree_totale_min', 'nb_arrets', 'distance_totale_km', 'vitesse_moy_kmh', 'est_nocturne'])
            ]
            
            for tableName, model, columns in tables:
                csvBuffer = io.StringIO()
                writer = csv.writer(csvBuffer)
                writer.writerow(columns)
                
                rows = db.session.query(model).all()
                for row in rows:
                    writer.writerow([getattr(row, col, '') for col in columns])
                
                zipf.writestr(f"{tableName}.csv", csvBuffer.getvalue())
            
            horairesCsv = io.StringIO()
            horaireWriter = csv.writer(horairesCsv)
            horaireWriter.writerow(['id_trajet', 'id_arret', 'sequence_arret', 'heure_arrivee', 'heure_depart'])
            
            horaires = db.session.query(HorairePassage).all()
            for h in horaires:
                horaireWriter.writerow([
                    h.id_trajet,
                    h.id_arret,
                    h.sequence_arret,
                    h.heure_arrivee.strftime('%H:%M:%S') if h.heure_arrivee else '',
                    h.heure_depart.strftime('%H:%M:%S') if h.heure_depart else ''
                ])
            
            zipf.writestr("stop_times.csv", horairesCsv.getvalue())
        
        memoryFile.seek(0)
        return send_file(
            memoryFile,
            mimetype='application/zip',
            as_attachment=True,
            download_name='database_export.zip'
        )
    except Exception as exc:
        current_app.logger.exception("CSV export failed")
        return jsonify({"error": "exportFailed", "details": str(exc)}), 500


@api.get("/export/csv/flat")
def exportFlatCsv():
    try:
        csvBuffer = io.StringIO()
        writer = csv.writer(csvBuffer)
        
        writer.writerow([
            'trip_id', 'trip_destination', 'trip_night',
            'route_short_name', 'route_long_name', 'route_type',
            'agency_name', 'agency_timezone',
            'stop_sequence', 'stop_name', 'stop_lat', 'stop_lon',
            'arrival_time', 'departure_time',
            'trip_distance_km', 'trip_duration_min', 'trip_avg_speed_kmh'
        ])
        
        query = db.session.query(
            Trajet.idTrajet,
            Trajet.destination,
            Trajet.trainDeNuit,
            Ligne.nomCourt,
            Ligne.nomLong,
            Ligne.typeLigne,
            Agence.nomAgence,
            Agence.fuseauHoraire,
            HorairePassage.sequenceArret,
            Arret.nomArret,
            Arret.latitude,
            Arret.longitude,
            HorairePassage.heureArrivee,
            HorairePassage.heureDepart,
            StatistiquesTrajet.distanceKm,
            StatistiquesTrajet.dureeMinutes,
            StatistiquesTrajet.vitesseMoyenneKmh
        ).join(
            Ligne, Trajet.idLigne == Ligne.idLigne
        ).join(
            Agence, Ligne.idAgence == Agence.idAgence
        ).join(
            HorairePassage, Trajet.idTrajet == HorairePassage.idTrajet
        ).join(
            Arret, HorairePassage.idArret == Arret.idArret
        ).outerjoin(
            StatistiquesTrajet, Trajet.idTrajet == StatistiquesTrajet.idTrajet
        ).order_by(
            Trajet.idTrajet, HorairePassage.sequenceArret
        )
        
        for row in query:
            writer.writerow([
                row[0],
                row[1] or '',
                1 if row[2] else 0,
                row[3] or '',
                row[4] or '',
                row[5] or '',
                row[6] or '',
                row[7] or '',
                row[8] or '',
                row[9] or '',
                row[10] or '',
                row[11] or '',
                row[12].strftime('%H:%M:%S') if row[12] else '',
                row[13].strftime('%H:%M:%S') if row[13] else '',
                row[14] or '',
                row[15] or '',
                row[16] or ''
            ])
        
        output = io.BytesIO()
        output.write(csvBuffer.getvalue().encode('utf-8-sig'))
        output.seek(0)
        
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name='trips_export.csv'
        )
    except Exception as exc:
        current_app.logger.exception("Flat CSV export failed")
        return jsonify({"error": "exportFailed", "details": str(exc)}), 500
