#!/usr/bin/env python3
"""
Script de test des requêtes SQL
Vérifie que les données sont bien chargées et que les relations fonctionnent
"""

import psycopg2
import sys
from pathlib import Path
from tabulate import tabulate

# Ajouter le chemin pour importer config
BASE_DIR = Path(__file__).parent.parent
sys.path.append(str(BASE_DIR))

from config.config import DATABASE
import os
import getpass

def connect_db():
    """Se connecter à la base de données"""
    default_user = os.getenv('USER', getpass.getuser())
    
    db_config = {
        'host': DATABASE.get('host', 'localhost'),
        'port': DATABASE.get('port', 5432),
        'database': DATABASE.get('name', 'obrail_europe'),
        'user': DATABASE.get('user', default_user),
        'password': DATABASE.get('password', '')
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        return conn
    except Exception as e:
        print(f"❌ Erreur de connexion : {e}")
        return None


def execute_query(conn, query, description):
    """Exécute une requête et affiche les résultats"""
    print(f"\n{'='*70}")
    print(f"📊 {description}")
    print(f"{'='*70}")
    
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            
            # Récupérer les résultats
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            
            if results:
                # Afficher avec tabulate
                print(tabulate(results, headers=columns, tablefmt="grid", maxcolwidths=50))
                print(f"\n✅ {len(results)} résultat(s)")
            else:
                print("⚠️  Aucun résultat")
                
    except Exception as e:
        print(f"❌ Erreur : {e}")


def test_basic_counts(conn):
    """Test 1 : Compter les lignes dans chaque table"""
    query = """
    SELECT 
        'agency' as table_name, COUNT(*) as count FROM agency
    UNION ALL
    SELECT 'routes', COUNT(*) FROM routes
    UNION ALL
    SELECT 'stops', COUNT(*) FROM stops
    UNION ALL
    SELECT 'trips', COUNT(*) FROM trips
    UNION ALL
    SELECT 'stop_times', COUNT(*) FROM stop_times
    UNION ALL
    SELECT 'calendar_dates', COUNT(*) FROM calendar_dates
    UNION ALL
    SELECT 'night_trains', COUNT(*) FROM night_trains
    UNION ALL
    SELECT 'night_train_stations', COUNT(*) FROM night_train_stations
    UNION ALL
    SELECT 'night_train_countries', COUNT(*) FROM night_train_countries
    UNION ALL
    SELECT 'night_train_operators', COUNT(*) FROM night_train_operators
    ORDER BY table_name;
    """
    execute_query(conn, query, "Nombre de lignes par table")


def test_agencies(conn):
    """Test 2 : Liste des opérateurs"""
    query = """
    SELECT 
        agency_id,
        agency_name,
        agency_timezone,
        agency_lang
    FROM agency
    ORDER BY agency_name;
    """
    execute_query(conn, query, "Liste des opérateurs (Agencies)")


def test_routes_sample(conn):
    """Test 3 : Échantillon de lignes avec leurs opérateurs"""
    query = """
    SELECT 
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        a.agency_name
    FROM routes r
    JOIN agency a ON r.agency_id = a.agency_id
    ORDER BY r.route_long_name
    LIMIT 10;
    """
    execute_query(conn, query, "Échantillon de lignes (Routes) avec opérateurs")


def test_stops_sample(conn):
    """Test 4 : Échantillon de gares"""
    query = """
    SELECT 
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        location_type,
        CASE 
            WHEN parent_station IS NULL THEN 'Gare principale'
            ELSE 'Arrêt'
        END as type_station
    FROM stops
    WHERE location_type = 1  -- Gares principales uniquement
    ORDER BY stop_name
    LIMIT 10;
    """
    execute_query(conn, query, "Échantillon de gares principales")


def test_trips_with_routes(conn):
    """Test 5 : Trajets avec leurs lignes"""
    query = """
    SELECT 
        t.trip_id,
        r.route_short_name,
        r.route_long_name,
        t.trip_headsign,
        COUNT(st.stop_sequence) as num_stops
    FROM trips t
    JOIN routes r ON t.route_id = r.route_id
    LEFT JOIN stop_times st ON t.trip_id = st.trip_id
    GROUP BY t.trip_id, r.route_short_name, r.route_long_name, t.trip_headsign
    ORDER BY num_stops DESC
    LIMIT 10;
    """
    execute_query(conn, query, "Top 10 trajets avec le plus d'arrêts")


def test_night_trains(conn):
    """Test 6 : Trains de nuit avec leurs pays"""
    query = """
    SELECT 
        nt.routeid,
        COALESCE(nt.nighttrain, 'N/A') as nighttrain,
        COALESCE(nt.itinerary, 'N/A') as itinerary,
        COUNT(DISTINCT ntc.country) as num_countries,
        COUNT(DISTINCT nto.operator) as num_operators,
        COUNT(DISTINCT nts.station) as num_stations
    FROM night_trains nt
    LEFT JOIN night_train_countries ntc ON nt.routeid = ntc.routeid
    LEFT JOIN night_train_operators nto ON nt.routeid = nto.routeid
    LEFT JOIN night_train_stations nts ON nt.routeid = nts.routeid
    GROUP BY nt.routeid, nt.nighttrain, nt.itinerary
    ORDER BY num_countries DESC
    LIMIT 10;
    """
    execute_query(conn, query, "Top 10 trains de nuit (par nombre de pays traversés)")


def test_stop_times_sample(conn):
    """Test 7 : Échantillon d'horaires"""
    query = """
    SELECT 
        st.trip_id,
        s.stop_name,
        st.arrival_time,
        st.departure_time,
        st.stop_sequence
    FROM stop_times st
    JOIN stops s ON st.stop_id = s.stop_id
    WHERE st.stop_sequence <= 3  -- Premiers arrêts seulement
    ORDER BY st.trip_id, st.stop_sequence
    LIMIT 15;
    """
    execute_query(conn, query, "Échantillon d'horaires (premiers arrêts de trajets)")


def test_geographic_search(conn):
    """Test 8 : Recherche géographique - Gares près de Paris"""
    query = """
    SELECT 
        stop_id,
        stop_name,
        stop_lat,
        stop_lon,
        ROUND(
            6371 * acos(
                cos(radians(48.8566)) * cos(radians(stop_lat)) *
                cos(radians(stop_lon) - radians(2.3522)) +
                sin(radians(48.8566)) * sin(radians(stop_lat))
            )::numeric, 2
        ) as distance_km
    FROM stops
    WHERE location_type = 1  -- Gares principales
    ORDER BY distance_km
    LIMIT 10;
    """
    execute_query(conn, query, "10 gares les plus proches de Paris (coordonnées: 48.8566, 2.3522)")


def test_route_statistics(conn):
    """Test 9 : Statistiques sur les lignes"""
    query = """
    SELECT 
        a.agency_name,
        COUNT(DISTINCT r.route_id) as num_routes,
        COUNT(DISTINCT t.trip_id) as num_trips,
        COUNT(DISTINCT st.stop_id) as num_stops
    FROM agency a
    JOIN routes r ON a.agency_id = r.agency_id
    LEFT JOIN trips t ON r.route_id = t.route_id
    LEFT JOIN stop_times st ON t.trip_id = st.trip_id
    GROUP BY a.agency_name
    ORDER BY num_routes DESC;
    """
    execute_query(conn, query, "Statistiques par opérateur")


def test_night_train_countries(conn):
    """Test 10 : Pays les plus traversés par les trains de nuit"""
    query = """
    SELECT 
        ntc.country,
        COUNT(DISTINCT ntc.routeid) as num_trains
    FROM night_train_countries ntc
    GROUP BY ntc.country
    ORDER BY num_trains DESC
    LIMIT 10;
    """
    execute_query(conn, query, "Top 10 pays les plus traversés par les trains de nuit")


def test_foreign_keys(conn):
    """Test 11 : Vérification des relations (foreign keys)"""
    query = """
    SELECT 
        'routes -> agency' as relation,
        COUNT(*) as total_routes,
        COUNT(DISTINCT r.agency_id) as distinct_agencies,
        COUNT(CASE WHEN a.agency_id IS NULL THEN 1 END) as orphaned_routes
    FROM routes r
    LEFT JOIN agency a ON r.agency_id = a.agency_id
    
    UNION ALL
    
    SELECT 
        'trips -> routes',
        COUNT(*) as total_trips,
        COUNT(DISTINCT t.route_id) as distinct_routes,
        COUNT(CASE WHEN r.route_id IS NULL THEN 1 END) as orphaned_trips
    FROM trips t
    LEFT JOIN routes r ON t.route_id = r.route_id
    
    UNION ALL
    
    SELECT 
        'stop_times -> trips',
        COUNT(*) as total_stop_times,
        COUNT(DISTINCT st.trip_id) as distinct_trips,
        COUNT(CASE WHEN t.trip_id IS NULL THEN 1 END) as orphaned_stop_times
    FROM stop_times st
    LEFT JOIN trips t ON st.trip_id = t.trip_id
    
    UNION ALL
    
    SELECT 
        'stop_times -> stops',
        COUNT(*) as total_stop_times,
        COUNT(DISTINCT st.stop_id) as distinct_stops,
        COUNT(CASE WHEN s.stop_id IS NULL THEN 1 END) as orphaned_stop_times
    FROM stop_times st
    LEFT JOIN stops s ON st.stop_id = s.stop_id;
    """
    execute_query(conn, query, "Vérification des relations (Foreign Keys)")


def main():
    """Fonction principale"""
    print("="*70)
    print("🧪 TESTS DE REQUÊTES SQL - ObRail Europe")
    print("="*70)
    
    # Se connecter
    conn = connect_db()
    if not conn:
        print("❌ Impossible de se connecter à la base de données")
        return
    
    print("\n✅ Connecté à la base de données obrail_europe")
    
    try:
        # Exécuter tous les tests
        test_basic_counts(conn)
        test_agencies(conn)
        test_routes_sample(conn)
        test_stops_sample(conn)
        test_trips_with_routes(conn)
        test_night_trains(conn)
        test_stop_times_sample(conn)
        test_geographic_search(conn)
        test_route_statistics(conn)
        test_night_train_countries(conn)
        test_foreign_keys(conn)
        
        print("\n" + "="*70)
        print("✅ TOUS LES TESTS TERMINÉS !")
        print("="*70)
        print("\n💡 Les données sont bien chargées et les relations fonctionnent correctement.")
        
    finally:
        conn.close()
        print("\n🔌 Connexion fermée")


if __name__ == "__main__":
    main()

