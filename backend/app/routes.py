from flask import Blueprint, jsonify, current_app, request
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import func, text
from .db import db
from .seeds import seed_dev_data
from .gtfs_importer import GTFSImporter
from .models import Trajet, Arret, HorairePassage, Agence, Ligne, StatistiquesTrajet

api = Blueprint("api", __name__)

@api.get("/test")
def test():
    return jsonify({"message": "API is working"})


@api.post("/initdb")
def init_db_route():
    if not current_app.config.get("ALLOW_INITDB", False):
        return jsonify({"error": "forbidden"}), 403

    try:
        db.create_all()
        return jsonify({"status": "database initialized"})
    except SQLAlchemyError as exc:
        current_app.logger.exception("DB init failed")
        return jsonify({"error": "db_init_failed", "details": str(exc)}), 500


@api.post("/import/seed")
def import_seed():
    """Load minimal dev/test data (SNCF, DB, day/night trips)."""
    if not current_app.config.get("ALLOW_INITDB", False):
        return jsonify({"error": "forbidden"}), 403

    try:
        seed_dev_data()
        return jsonify({"status": "seed data imported", "agencies": 2, "trips": 3})
    except Exception as exc:
        current_app.logger.exception("Seed import failed")
        return jsonify({"error": "seed_import_failed", "details": str(exc)}), 500


@api.post("/import/gtfs")
def import_gtfs():
    """Import GTFS feed from URL or uploaded file.
    
    Query params:
    - url: HTTP URL to GTFS ZIP (e.g., ?url=https://example.com/feed.zip)
    
    Or upload a file (multipart/form-data):
    - file: GTFS ZIP file
    
    Examples:
    - curl -X POST "http://localhost:5001/import/gtfs?url=https://..."
    - curl -F "file=@test_gtfs.zip" http://localhost:5001/import/gtfs
    """
    if not current_app.config.get("ALLOW_INITDB", False):
        return jsonify({"error": "forbidden"}), 403

    gtfs_url = request.args.get("url")
    gtfs_file = request.files.get("file")

    if not gtfs_url and not gtfs_file:
        return jsonify({
            "error": "missing_input",
            "details": "Provide either ?url=<url> query param or upload file"
        }), 400

    summary = None

    if gtfs_file:
        # Read stream into memory so we can inspect then import
        import io, zipfile
        file_bytes = gtfs_file.read()
        importer = GTFSImporter(None)
        try:
            importer.zip_file = zipfile.ZipFile(io.BytesIO(file_bytes))
        except zipfile.BadZipFile as e:
            return jsonify({"error": "invalid_zip", "details": str(e)}), 400
        summary = importer.inspect_feed()
        success = importer.parse_and_import()
    else:
        # Handle URL with preflight inspection
        importer = GTFSImporter(gtfs_url)
        if not importer.download_and_parse():
            return jsonify({"error": "download_failed", "details": importer.errors}), 400
        summary = importer.inspect_feed()
        success = importer.parse_and_import()

    if success:
        note = None
        if summary and not summary.get("has_core_schedules"):
            note = "Feed contains no trips/stop_times. Routes and stops are imported; trips will be empty. Use /import/gtfs/preflight to verify feeds before import."
        return jsonify({
            "status": "gtfs_imported",
            "preflight": summary,
            "stats": importer.stats,
            "note": note,
            "errors": importer.errors
        }), 200
    else:
        return jsonify({
            "error": "gtfs_import_failed",
            "details": importer.errors,
            "preflight": summary
        }), 500


@api.get("/import/gtfs/preflight")
def import_gtfs_preflight():
    """Inspect a GTFS feed by URL and report file availability + row counts.

    Query params:
    - url: HTTP URL to GTFS ZIP

    Returns: summary dict indicating presence and counts for agency, routes, stops, calendar, trips, stop_times.
    """
    if not current_app.config.get("ALLOW_INITDB", False):
        return jsonify({"error": "forbidden"}), 403

    gtfs_url = request.args.get("url")
    if not gtfs_url:
        return jsonify({"error": "missing_url", "details": "?url=https://..."}), 400

    importer = GTFSImporter(gtfs_url)
    if not importer.download_and_parse():
        return jsonify({"error": "download_failed", "details": importer.errors}), 400

    summary = importer.inspect_feed()
    return jsonify({
        "status": "gtfs_preflight",
        "summary": summary
    }), 200


@api.get("/trajets")
def list_trajets():
    """List trips with optional filters.
    
    Query params:
    - agency_id: filter by agency (id_agence)
    - is_night: filter by day/night (true/false)
    - limit: max results (default 50)
    - offset: pagination offset (default 0)
    
    Example: GET /trajets?is_night=false&limit=20
    """
    try:
        # Build query
        query = db.session.query(Trajet).join(Ligne).join(Agence)

        # Filters
        agency_id = request.args.get("agency_id", type=int)
        if agency_id:
            query = query.filter(Ligne.id_agence == agency_id)

        is_night = request.args.get("is_night")
        if is_night is not None:
            is_night_bool = is_night.lower() == "true"
            query = query.filter(Trajet.train_de_nuit == is_night_bool)

        # Pagination
        limit = request.args.get("limit", default=50, type=int)
        offset = request.args.get("offset", default=0, type=int)
        limit = min(limit, 200)  # cap at 200

        total = query.count()
        trajets = query.offset(offset).limit(limit).all()

        result = {
            "total": total,
            "limit": limit,
            "offset": offset,
            "trajets": [
                {
                    "id": t.id_trajet,
                    "ligne": {
                        "id": t.ligne.id_ligne,
                        "nom_long": t.ligne.nom_long,
                        "nom_court": t.ligne.nom_court,
                        "type": t.ligne.type_ligne,
                    },
                    "agence": {
                        "id": t.ligne.agence.id_agence,
                        "nom": t.ligne.agence.nom_agence,
                    },
                    "destination": t.destination,
                    "train_de_nuit": t.train_de_nuit,
                    "horaires": [
                        {
                            "sequence": h.sequence_arret,
                            "arret": h.arret.nom_arret,
                            "arrival": h.heure_arrivee.strftime("%H:%M") if h.heure_arrivee else None,
                            "departure": h.heure_depart.strftime("%H:%M") if h.heure_depart else None,
                        }
                        for h in sorted(t.horaires, key=lambda x: x.sequence_arret)
                    ],
                    "stats": {
                        "distance_km": float(t.stats.distance_km) if t.stats and t.stats.distance_km else None,
                        "duree_minutes": t.stats.duree_minutes if t.stats else None,
                        "co2_total_g": float(t.stats.co2_total_g) if t.stats and t.stats.co2_total_g else None,
                        "co2_par_passager_g": float(t.stats.co2_par_passager_g) if t.stats and t.stats.co2_par_passager_g else None,
                    } if t.stats else None,
                }
                for t in trajets
            ]
        }

        return jsonify(result), 200

    except Exception as exc:
        current_app.logger.exception("Error listing trajets")
        return jsonify({"error": "list_trajets_failed", "details": str(exc)}), 500


@api.get("/trajets/<int:trajet_id>")
def get_trajet(trajet_id):
    """Get trip details by ID."""
    try:
        trajet = db.session.query(Trajet).filter(Trajet.id_trajet == trajet_id).first()
        if not trajet:
            return jsonify({"error": "not_found"}), 404

        result = {
            "id": trajet.id_trajet,
            "ligne": {
                "id": trajet.ligne.id_ligne,
                "nom_long": trajet.ligne.nom_long,
                "nom_court": trajet.ligne.nom_court,
                "type": trajet.ligne.type_ligne,
            },
            "agence": {
                "id": trajet.ligne.agence.id_agence,
                "nom": trajet.ligne.agence.nom_agence,
                "url": trajet.ligne.agence.url,
                "fuseau_horaire": trajet.ligne.agence.fuseau_horaire,
            },
            "destination": trajet.destination,
            "train_de_nuit": trajet.train_de_nuit,
            "horaires": [
                {
                    "sequence": h.sequence_arret,
                    "arret": {
                        "id": h.arret.id_arret,
                        "nom": h.arret.nom_arret,
                        "latitude": float(h.arret.latitude),
                        "longitude": float(h.arret.longitude),
                    },
                    "arrival": h.heure_arrivee.strftime("%H:%M") if h.heure_arrivee else None,
                    "departure": h.heure_depart.strftime("%H:%M") if h.heure_depart else None,
                }
                for h in sorted(trajet.horaires, key=lambda x: x.sequence_arret)
            ],
            "stats": {
                "distance_km": float(trajet.stats.distance_km) if trajet.stats and trajet.stats.distance_km else None,
                "duree_minutes": trajet.stats.duree_minutes if trajet.stats else None,
                "vitesse_moyenne_kmh": float(trajet.stats.vitesse_moyenne_kmh) if trajet.stats and trajet.stats.vitesse_moyenne_kmh else None,
                "co2_total_g": float(trajet.stats.co2_total_g) if trajet.stats and trajet.stats.co2_total_g else None,
                "co2_par_passager_g": float(trajet.stats.co2_par_passager_g) if trajet.stats and trajet.stats.co2_par_passager_g else None,
            } if trajet.stats else None,
        }

        return jsonify(result), 200

    except Exception as exc:
        current_app.logger.exception(f"Error fetching trajet {trajet_id}")
        return jsonify({"error": "get_trajet_failed", "details": str(exc)}), 500


@api.get("/stats/volumes")
def stats_volumes():
    """Get aggregated trip statistics by agency and day/night.
    
    Query params:
    - agency_id: filter by agency (optional)
    
    Returns: volumes grouped by (agency, is_night)
    """
    try:
        from sqlalchemy import cast, Float
        # Query starting from Trajet with proper joins
        query = db.session.query(
            Agence.id_agence,
            Agence.nom_agence,
            Trajet.train_de_nuit,
            func.count(Trajet.id_trajet).label("trip_count"),
            func.coalesce(func.sum(cast(StatistiquesTrajet.distance_km, Float)), 0).label("total_distance_km"),
            func.coalesce(func.avg(cast(StatistiquesTrajet.distance_km, Float)), 0).label("avg_distance_km"),
            func.coalesce(func.sum(cast(StatistiquesTrajet.co2_total_g, Float)), 0).label("total_co2_g"),
            func.coalesce(func.avg(cast(StatistiquesTrajet.co2_par_passager_g, Float)), 0).label("avg_co2_per_pax_g"),
        ).select_from(Trajet).join(
            Ligne, Trajet.id_ligne == Ligne.id_ligne
        ).join(
            Agence, Ligne.id_agence == Agence.id_agence
        ).outerjoin(
            StatistiquesTrajet, StatistiquesTrajet.id_trajet == Trajet.id_trajet
        ).group_by(
            Agence.id_agence, Agence.nom_agence, Trajet.train_de_nuit
        )

        # Optional filter by agency
        agency_id = request.args.get("agency_id", type=int)
        if agency_id:
            query = query.filter(Agence.id_agence == agency_id)

        results = query.all()

        stats = {
            "summary": {
                "total_agencies": len(set(r[0] for r in results)),
                "total_trips": sum(r[3] for r in results),
            },
            "by_agency": []
        }

        # Group by agency
        by_agency = {}
        for row in results:
            agency_id, agency_name, is_night, trip_count, total_dist, avg_dist, total_co2, avg_co2 = row
            key = f"{agency_id}:{agency_name}"
            if key not in by_agency:
                by_agency[key] = {
                    "agency_id": agency_id,
                    "agency_name": agency_name,
                    "day_trips": None,
                    "night_trips": None,
                }
            if is_night:
                by_agency[key]["night_trips"] = {
                    "count": int(trip_count),
                    "total_distance_km": float(total_dist) if total_dist else 0,
                    "avg_distance_km": float(avg_dist) if avg_dist else 0,
                    "total_co2_g": float(total_co2) if total_co2 else 0,
                    "avg_co2_per_passenger_g": float(avg_co2) if avg_co2 else 0,
                }
            else:
                by_agency[key]["day_trips"] = {
                    "count": int(trip_count),
                    "total_distance_km": float(total_dist) if total_dist else 0,
                    "avg_distance_km": float(avg_dist) if avg_dist else 0,
                    "total_co2_g": float(total_co2) if total_co2 else 0,
                    "avg_co2_per_passenger_g": float(avg_co2) if avg_co2 else 0,
                }

        stats["by_agency"] = list(by_agency.values())
        return jsonify(stats), 200

    except Exception as exc:
        current_app.logger.exception("Error computing stats/volumes")
        return jsonify({"error": "stats_failed", "details": str(exc)}), 500


@api.get("/health")
def health():
    """Health check endpoint."""
    try:
        # Test DB connection
        db.session.execute(text("SELECT 1"))
        return jsonify({"status": "healthy", "database": "ok"}), 200
    except Exception as exc:
        current_app.logger.error(f"Health check failed: {exc}")
        return jsonify({"status": "unhealthy", "database": "error", "details": str(exc)}), 503
