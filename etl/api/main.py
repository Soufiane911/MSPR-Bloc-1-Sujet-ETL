"""
API REST pour ObRail Europe
FastAPI avec endpoints pour interroger les données ferroviaires
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor

from .database import get_db_connection, get_db_cursor
from .models import (
    Agency, Route, RouteStats, Stop, StopPopularity,
    Trip, TripEnriched, StopTime, StopTimeWithDetails,
    NightTrain, NightTrainEnriched, NightTrainStation,
    Message, PaginatedResponse
)

# Créer l'application FastAPI
app = FastAPI(
    title="ObRail Europe API",
    description="API REST pour interroger les données ferroviaires européennes (GTFS SNCF et trains de nuit Back-on-Track)",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En production, spécifier les origines autorisées
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Endpoints GTFS - Opérateurs (Agencies)
# ============================================================================

@app.get("/api/agencies", response_model=List[Agency], tags=["GTFS - Opérateurs"])
async def get_agencies():
    """
    Récupère la liste de tous les opérateurs
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM agency ORDER BY agency_name")
        agencies = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(agency) for agency in agencies]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/agencies/{agency_id}", response_model=Agency, tags=["GTFS - Opérateurs"])
async def get_agency(agency_id: str):
    """
    Récupère les détails d'un opérateur par son ID
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM agency WHERE agency_id = %s", (agency_id,))
        agency = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not agency:
            raise HTTPException(status_code=404, detail="Opérateur non trouvé")
        
        return dict(agency)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Endpoints GTFS - Lignes (Routes)
# ============================================================================

@app.get("/api/routes", response_model=List[Route], tags=["GTFS - Lignes"])
async def get_routes(
    agency_id: Optional[str] = Query(None, description="Filtrer par opérateur"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats")
):
    """
    Récupère la liste des lignes de train
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        if agency_id:
            cur.execute(
                "SELECT * FROM routes WHERE agency_id = %s ORDER BY route_long_name LIMIT %s",
                (agency_id, limit)
            )
        else:
            cur.execute("SELECT * FROM routes ORDER BY route_long_name LIMIT %s", (limit,))
        
        routes = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(route) for route in routes]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# IMPORTANT: Les routes spécifiques doivent être définies AVANT les routes avec paramètres
@app.get("/api/routes/stats", response_model=List[RouteStats], tags=["GTFS - Lignes"])
async def get_routes_stats(
    limit: int = Query(50, ge=1, le=500, description="Nombre maximum de résultats")
):
    """
    Récupère les statistiques des lignes (vue Gold)
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM v_routes_stats ORDER BY num_trips DESC LIMIT %s", (limit,))
        stats = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(stat) for stat in stats]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/routes/top", response_model=List[RouteStats], tags=["GTFS - Lignes"])
async def get_top_routes(
    limit: int = Query(10, ge=1, le=50, description="Nombre de résultats")
):
    """
    Récupère les lignes les plus actives (vue Gold)
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM v_top_routes_by_trips LIMIT %s", (limit,))
        routes = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(route) for route in routes]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/routes/{route_id}", response_model=Route, tags=["GTFS - Lignes"])
async def get_route(route_id: str):
    """
    Récupère les détails d'une ligne par son ID
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM routes WHERE route_id = %s", (route_id,))
        route = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not route:
            raise HTTPException(status_code=404, detail="Ligne non trouvée")
        
        return dict(route)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Endpoints GTFS - Gares (Stops)
# ============================================================================

@app.get("/api/stops", response_model=List[Stop], tags=["GTFS - Gares"])
async def get_stops(
    search: Optional[str] = Query(None, description="Rechercher par nom de gare"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats")
):
    """
    Récupère la liste des gares
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        if search:
            cur.execute(
                "SELECT * FROM stops WHERE stop_name ILIKE %s ORDER BY stop_name LIMIT %s",
                (f"%{search}%", limit)
            )
        else:
            cur.execute("SELECT * FROM stops ORDER BY stop_name LIMIT %s", (limit,))
        
        stops = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(stop) for stop in stops]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# IMPORTANT: Les routes spécifiques doivent être définies AVANT les routes avec paramètres
@app.get("/api/stops/popularity", response_model=List[StopPopularity], tags=["GTFS - Gares"])
async def get_stops_popularity(
    limit: int = Query(50, ge=1, le=500, description="Nombre maximum de résultats")
):
    """
    Récupère les gares les plus fréquentées (vue Gold)
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM v_stops_popularity LIMIT %s", (limit,))
        stops = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(stop) for stop in stops]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stops/top", response_model=List[StopPopularity], tags=["GTFS - Gares"])
async def get_top_stops(
    limit: int = Query(10, ge=1, le=50, description="Nombre de résultats")
):
    """
    Récupère les gares les plus fréquentées (vue Gold - Top)
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM v_top_stops_by_popularity LIMIT %s", (limit,))
        stops = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(stop) for stop in stops]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stops/{stop_id}", response_model=Stop, tags=["GTFS - Gares"])
async def get_stop(stop_id: str):
    """
    Récupère les détails d'une gare par son ID
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM stops WHERE stop_id = %s", (stop_id,))
        stop = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not stop:
            raise HTTPException(status_code=404, detail="Gare non trouvée")
        
        return dict(stop)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Endpoints GTFS - Trajets (Trips)
# ============================================================================

@app.get("/api/trips", response_model=List[Trip], tags=["GTFS - Trajets"])
async def get_trips(
    route_id: Optional[str] = Query(None, description="Filtrer par ligne"),
    limit: int = Query(100, ge=1, le=1000, description="Nombre maximum de résultats")
):
    """
    Récupère la liste des trajets
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        if route_id:
            cur.execute(
                "SELECT * FROM trips WHERE route_id = %s ORDER BY trip_id LIMIT %s",
                (route_id, limit)
            )
        else:
            cur.execute("SELECT * FROM trips ORDER BY trip_id LIMIT %s", (limit,))
        
        trips = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(trip) for trip in trips]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# IMPORTANT: Les routes spécifiques doivent être définies AVANT les routes avec paramètres
@app.get("/api/trips/longest", response_model=List[TripEnriched], tags=["GTFS - Trajets"])
async def get_longest_trips(
    limit: int = Query(10, ge=1, le=50, description="Nombre de résultats")
):
    """
    Récupère les trajets les plus longs (par nombre d'arrêts)
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM v_longest_trips LIMIT %s", (limit,))
        trips = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(trip) for trip in trips]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trips/{trip_id}", response_model=Trip, tags=["GTFS - Trajets"])
async def get_trip(trip_id: str):
    """
    Récupère les détails d'un trajet par son ID
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM trips WHERE trip_id = %s", (trip_id,))
        trip = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not trip:
            raise HTTPException(status_code=404, detail="Trajet non trouvé")
        
        return dict(trip)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trips/{trip_id}/enriched", response_model=TripEnriched, tags=["GTFS - Trajets"])
async def get_trip_enriched(trip_id: str):
    """
    Récupère les détails enrichis d'un trajet (vue Gold)
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM v_trips_enriched WHERE trip_id = %s", (trip_id,))
        trip = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not trip:
            raise HTTPException(status_code=404, detail="Trajet non trouvé")
        
        return dict(trip)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/trips/{trip_id}/stops", response_model=List[StopTimeWithDetails], tags=["GTFS - Trajets"])
async def get_trip_stops(trip_id: str):
    """
    Récupère tous les arrêts d'un trajet avec les détails des gares
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("""
            SELECT 
                st.trip_id,
                st.arrival_time,
                st.departure_time,
                st.stop_id,
                s.stop_name,
                st.stop_sequence,
                st.pickup_type,
                st.drop_off_type
            FROM stop_times st
            JOIN stops s ON st.stop_id = s.stop_id
            WHERE st.trip_id = %s
            ORDER BY st.stop_sequence
        """, (trip_id,))
        
        stops = cur.fetchall()
        
        cur.close()
        conn.close()
        
        if not stops:
            raise HTTPException(status_code=404, detail="Trajet non trouvé ou aucun arrêt")
        
        return [dict(stop) for stop in stops]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Endpoints Back-on-Track - Trains de nuit
# ============================================================================

@app.get("/api/night-trains", response_model=List[NightTrain], tags=["Back-on-Track"])
async def get_night_trains(
    limit: int = Query(100, ge=1, le=500, description="Nombre maximum de résultats")
):
    """
    Récupère la liste des trains de nuit
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM night_trains ORDER BY nighttrain LIMIT %s", (limit,))
        trains = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(train) for train in trains]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/night-trains/{routeid}", response_model=NightTrain, tags=["Back-on-Track"])
async def get_night_train(routeid: str):
    """
    Récupère les détails d'un train de nuit par son ID
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM night_trains WHERE routeid = %s", (routeid,))
        train = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not train:
            raise HTTPException(status_code=404, detail="Train de nuit non trouvé")
        
        return dict(train)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/night-trains/{routeid}/enriched", response_model=NightTrainEnriched, tags=["Back-on-Track"])
async def get_night_train_enriched(routeid: str):
    """
    Récupère les détails enrichis d'un train de nuit (vue Gold)
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM v_night_trains_enriched WHERE routeid = %s", (routeid,))
        train = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not train:
            raise HTTPException(status_code=404, detail="Train de nuit non trouvé")
        
        return dict(train)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/night-trains/international", response_model=List[NightTrainEnriched], tags=["Back-on-Track"])
async def get_international_night_trains(
    limit: int = Query(10, ge=1, le=50, description="Nombre de résultats")
):
    """
    Récupère les trains de nuit les plus internationaux (vue Gold)
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("SELECT * FROM v_most_international_night_trains LIMIT %s", (limit,))
        trains = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return [dict(train) for train in trains]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/night-trains/{routeid}/stations", response_model=List[NightTrainStation], tags=["Back-on-Track"])
async def get_night_train_stations(routeid: str):
    """
    Récupère toutes les gares d'un train de nuit
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        cur.execute("""
            SELECT * FROM night_train_stations 
            WHERE routeid = %s 
            ORDER BY direction, sequence
        """, (routeid,))
        
        stations = cur.fetchall()
        
        cur.close()
        conn.close()
        
        if not stations:
            raise HTTPException(status_code=404, detail="Train de nuit non trouvé ou aucune gare")
        
        return [dict(station) for station in stations]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Endpoints de recherche et statistiques
# ============================================================================

@app.get("/api/search", tags=["Recherche"])
async def search(
    q: str = Query(..., description="Terme de recherche"),
    type: Optional[str] = Query(None, description="Type: 'stops', 'routes', 'night-trains'"),
    limit: int = Query(20, ge=1, le=100, description="Nombre maximum de résultats")
):
    """
    Recherche globale dans les gares, lignes et trains de nuit
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        results = {}
        
        if not type or type == "stops":
            cur.execute("""
                SELECT stop_id, stop_name, 'stop' as type 
                FROM stops 
                WHERE stop_name ILIKE %s 
                LIMIT %s
            """, (f"%{q}%", limit))
            results["stops"] = [dict(row) for row in cur.fetchall()]
        
        if not type or type == "routes":
            cur.execute("""
                SELECT route_id, route_long_name as name, 'route' as type 
                FROM routes 
                WHERE route_long_name ILIKE %s 
                LIMIT %s
            """, (f"%{q}%", limit))
            results["routes"] = [dict(row) for row in cur.fetchall()]
        
        if not type or type == "night-trains":
            cur.execute("""
                SELECT routeid as id, nighttrain as name, 'night-train' as type 
                FROM night_trains 
                WHERE nighttrain ILIKE %s OR itinerary ILIKE %s
                LIMIT %s
            """, (f"%{q}%", f"%{q}%", limit))
            results["night_trains"] = [dict(row) for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats", tags=["Statistiques"])
async def get_stats():
    """
    Récupère les statistiques globales de la base de données
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        
        stats = {}
        
        # Comptages GTFS
        cur.execute("SELECT COUNT(*) as count FROM agency")
        stats["agencies"] = cur.fetchone()["count"]
        
        cur.execute("SELECT COUNT(*) as count FROM routes")
        stats["routes"] = cur.fetchone()["count"]
        
        cur.execute("SELECT COUNT(*) as count FROM stops")
        stats["stops"] = cur.fetchone()["count"]
        
        cur.execute("SELECT COUNT(*) as count FROM trips")
        stats["trips"] = cur.fetchone()["count"]
        
        cur.execute("SELECT COUNT(*) as count FROM stop_times")
        stats["stop_times"] = cur.fetchone()["count"]
        
        # Comptages Back-on-Track
        cur.execute("SELECT COUNT(*) as count FROM night_trains")
        stats["night_trains"] = cur.fetchone()["count"]
        
        cur.execute("SELECT COUNT(DISTINCT country) as count FROM night_train_countries")
        stats["night_train_countries"] = cur.fetchone()["count"]
        
        cur.close()
        conn.close()
        
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Endpoint racine
# ============================================================================

@app.get("/", response_model=Message, tags=["Général"])
async def root():
    """
    Endpoint racine de l'API
    """
    return {"message": "Bienvenue sur l'API ObRail Europe ! Consultez /docs pour la documentation."}


@app.get("/health", tags=["Général"])
async def health_check():
    """
    Vérification de l'état de l'API et de la base de données
    """
    try:
        conn = get_db_connection()
        cur = get_db_cursor(conn)
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e)}

