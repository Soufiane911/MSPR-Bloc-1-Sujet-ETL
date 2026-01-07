-- ============================================================================
-- COUCHE GOLD : Vues agrégées et enrichies pour l'analyse et l'API
-- ============================================================================
-- Ces vues contiennent des données pré-calculées et enrichies
-- pour améliorer les performances de l'API et faciliter l'analyse
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Vue : Statistiques par ligne (Routes)
-- Description : Métriques agrégées pour chaque ligne de train
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_routes_stats AS
SELECT 
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    a.agency_name,
    COUNT(DISTINCT t.trip_id) as num_trips,
    COUNT(DISTINCT st.stop_id) as num_stops,
    COUNT(DISTINCT st.trip_id) as num_trips_with_stops,
    AVG(stop_count.stop_count) as avg_stops_per_trip,
    MIN(stop_count.stop_count) as min_stops_per_trip,
    MAX(stop_count.stop_count) as max_stops_per_trip
FROM routes r
JOIN agency a ON r.agency_id = a.agency_id
LEFT JOIN trips t ON r.route_id = t.route_id
LEFT JOIN stop_times st ON t.trip_id = st.trip_id
LEFT JOIN (
    SELECT trip_id, COUNT(*) as stop_count 
    FROM stop_times 
    GROUP BY trip_id
) stop_count ON t.trip_id = stop_count.trip_id
GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type, a.agency_name;

COMMENT ON VIEW v_routes_stats IS 'Statistiques agrégées par ligne de train (nombre de trajets, gares, etc.)';

-- ----------------------------------------------------------------------------
-- Vue : Gares les plus fréquentées
-- Description : Métriques de fréquentation pour chaque gare
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_stops_popularity AS
SELECT 
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    s.location_type,
    COUNT(DISTINCT st.trip_id) as num_trips,
    COUNT(DISTINCT t.route_id) as num_routes,
    COUNT(DISTINCT a.agency_id) as num_agencies,
    COUNT(*) as total_stop_times
FROM stops s
JOIN stop_times st ON s.stop_id = st.stop_id
JOIN trips t ON st.trip_id = t.trip_id
JOIN routes r ON t.route_id = r.route_id
JOIN agency a ON r.agency_id = a.agency_id
WHERE s.location_type = 1  -- Gares principales uniquement
GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon, s.location_type
ORDER BY num_trips DESC;

COMMENT ON VIEW v_stops_popularity IS 'Gares classées par fréquentation (nombre de trajets, lignes, etc.)';

-- ----------------------------------------------------------------------------
-- Vue : Trajets enrichis
-- Description : Trajets avec métriques calculées (durée, nombre d'arrêts)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_trips_enriched AS
SELECT 
    t.trip_id,
    t.route_id,
    r.route_short_name,
    r.route_long_name,
    t.trip_headsign,
    t.direction_id,
    a.agency_name,
    COUNT(DISTINCT st.stop_id) as num_stops,
    MIN(st.stop_sequence) as first_stop_sequence,
    MAX(st.stop_sequence) as last_stop_sequence,
    MIN(st.arrival_time) as first_arrival_time,
    MAX(st.departure_time) as last_departure_time,
    COUNT(DISTINCT cd.date) as num_service_dates
FROM trips t
JOIN routes r ON t.route_id = r.route_id
JOIN agency a ON r.agency_id = a.agency_id
LEFT JOIN stop_times st ON t.trip_id = st.trip_id
LEFT JOIN calendar_dates cd ON t.service_id = cd.service_id
GROUP BY t.trip_id, t.route_id, r.route_short_name, r.route_long_name, 
         t.trip_headsign, t.direction_id, a.agency_name;

COMMENT ON VIEW v_trips_enriched IS 'Trajets avec métriques enrichies (nombre d''arrêts, horaires, etc.)';

-- ----------------------------------------------------------------------------
-- Vue : Trains de nuit enrichis
-- Description : Trains de nuit avec statistiques agrégées
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_night_trains_enriched AS
SELECT 
    nt.routeid,
    nt.nighttrain,
    nt.itinerary,
    nt.routelongname,
    COUNT(DISTINCT ntc.country) as num_countries,
    COUNT(DISTINCT nto.operator) as num_operators,
    COUNT(DISTINCT nts.station) as num_stations,
    COUNT(DISTINCT CASE WHEN nts.direction = 1 THEN nts.station END) as num_stations_direction_1,
    COUNT(DISTINCT CASE WHEN nts.direction = 2 THEN nts.station END) as num_stations_direction_2,
    STRING_AGG(DISTINCT ntc.country, ', ' ORDER BY ntc.country) as countries_list,
    STRING_AGG(DISTINCT nto.operator, ', ' ORDER BY nto.operator) as operators_list
FROM night_trains nt
LEFT JOIN night_train_countries ntc ON nt.routeid = ntc.routeid
LEFT JOIN night_train_operators nto ON nt.routeid = nto.routeid
LEFT JOIN night_train_stations nts ON nt.routeid = nts.routeid
GROUP BY nt.routeid, nt.nighttrain, nt.itinerary, nt.routelongname;

COMMENT ON VIEW v_night_trains_enriched IS 'Trains de nuit avec statistiques agrégées (pays, opérateurs, gares)';

-- ----------------------------------------------------------------------------
-- Vue : Statistiques par opérateur (Agency)
-- Description : Métriques agrégées pour chaque opérateur
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_agency_stats AS
SELECT 
    a.agency_id,
    a.agency_name,
    a.agency_timezone,
    COUNT(DISTINCT r.route_id) as num_routes,
    COUNT(DISTINCT t.trip_id) as num_trips,
    COUNT(DISTINCT st.stop_id) as num_stops,
    COUNT(DISTINCT st.trip_id) as num_trips_with_stops,
    COUNT(*) as total_stop_times
FROM agency a
LEFT JOIN routes r ON a.agency_id = r.agency_id
LEFT JOIN trips t ON r.route_id = t.route_id
LEFT JOIN stop_times st ON t.trip_id = st.trip_id
GROUP BY a.agency_id, a.agency_name, a.agency_timezone
ORDER BY num_routes DESC;

COMMENT ON VIEW v_agency_stats IS 'Statistiques agrégées par opérateur (nombre de lignes, trajets, gares)';

-- ----------------------------------------------------------------------------
-- Vue : Top lignes par nombre de trajets
-- Description : Classement des lignes les plus actives
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_top_routes_by_trips AS
SELECT 
    route_id,
    route_short_name,
    route_long_name,
    agency_name,
    num_trips,
    num_stops,
    avg_stops_per_trip
FROM v_routes_stats
ORDER BY num_trips DESC
LIMIT 50;

COMMENT ON VIEW v_top_routes_by_trips IS 'Top 50 lignes par nombre de trajets';

-- ----------------------------------------------------------------------------
-- Vue : Top gares par fréquentation
-- Description : Classement des gares les plus fréquentées
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_top_stops_by_popularity AS
SELECT 
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    num_trips,
    num_routes,
    num_agencies,
    total_stop_times
FROM v_stops_popularity
ORDER BY num_trips DESC
LIMIT 50;

COMMENT ON VIEW v_top_stops_by_popularity IS 'Top 50 gares par fréquentation';

-- ----------------------------------------------------------------------------
-- Vue : Trajets les plus longs (par nombre d'arrêts)
-- Description : Classement des trajets avec le plus d'arrêts
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_longest_trips AS
SELECT 
    trip_id,
    route_short_name,
    route_long_name,
    trip_headsign,
    agency_name,
    num_stops,
    first_arrival_time,
    last_departure_time
FROM v_trips_enriched
ORDER BY num_stops DESC
LIMIT 50;

COMMENT ON VIEW v_longest_trips IS 'Top 50 trajets les plus longs (par nombre d''arrêts)';

-- ----------------------------------------------------------------------------
-- Vue : Trains de nuit les plus internationaux
-- Description : Classement des trains de nuit traversant le plus de pays
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_most_international_night_trains AS
SELECT 
    routeid,
    nighttrain,
    itinerary,
    num_countries,
    num_operators,
    num_stations,
    countries_list,
    operators_list
FROM v_night_trains_enriched
ORDER BY num_countries DESC, num_operators DESC
LIMIT 20;

COMMENT ON VIEW v_most_international_night_trains IS 'Top 20 trains de nuit les plus internationaux';

-- ============================================================================
-- Index pour optimiser les vues (si nécessaire)
-- ============================================================================

-- Les index sur les tables de base sont déjà créés dans schema.sql
-- Les vues utilisent ces index automatiquement

-- ============================================================================
-- FIN DU SCRIPT
-- ============================================================================

