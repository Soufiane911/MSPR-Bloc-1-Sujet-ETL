-- Vues SQL pour la comparaison équitable jour/nuit
-- Ces vues normalisent les données pour permettre une analyse pertinente
-- Vue 1: Distribution par source et type
CREATE OR REPLACE VIEW v_distribution_by_source AS
SELECT source_name,
    train_type,
    COUNT(*) as nb_trains,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM trains
GROUP BY source_name,
    train_type
ORDER BY nb_trains DESC;
-- Vue 2: Distribution par pays et type
CREATE OR REPLACE VIEW v_distribution_by_country AS
SELECT COALESCE(o.country, 'Unknown') as country,
    t.train_type,
    COUNT(*) as nb_trains,
    COUNT(DISTINCT t.operator_id) as nb_operators
FROM trains t
    LEFT JOIN operators o ON t.operator_id = o.operator_id
GROUP BY o.country,
    t.train_type
ORDER BY nb_trains DESC;
-- Vue 3: Opérateurs de trains de nuit
CREATE OR REPLACE VIEW v_night_train_operators AS
SELECT o.name as operator_name,
    o.country,
    COUNT(*) as nb_night_trains,
    COUNT(DISTINCT t.route_name) as nb_routes
FROM trains t
    JOIN operators o ON t.operator_id = o.operator_id
WHERE t.train_type = 'night'
GROUP BY o.name,
    o.country
ORDER BY nb_night_trains DESC;
-- Vue 4: Opérateurs ayant des trains jour ET nuit (comparaison équitable)
CREATE OR REPLACE VIEW v_operators_mixed AS WITH operator_stats AS (
        SELECT o.operator_id,
            o.name as operator_name,
            o.country,
            SUM(
                CASE
                    WHEN t.train_type = 'day' THEN 1
                    ELSE 0
                END
            ) as day_trains,
            SUM(
                CASE
                    WHEN t.train_type = 'night' THEN 1
                    ELSE 0
                END
            ) as night_trains,
            COUNT(*) as total_trains
        FROM trains t
            JOIN operators o ON t.operator_id = o.operator_id
        GROUP BY o.operator_id,
            o.name,
            o.country
    )
SELECT operator_name,
    country,
    day_trains,
    night_trains,
    total_trains,
    ROUND(
        night_trains * 100.0 / NULLIF(total_trains, 0),
        1
    ) as night_percentage
FROM operator_stats
WHERE day_trains > 0
    AND night_trains > 0
ORDER BY night_percentage DESC;
-- Vue 5: Résumé global par type
CREATE OR REPLACE VIEW v_summary_by_type AS
SELECT train_type,
    COUNT(*) as nb_trains,
    COUNT(DISTINCT operator_id) as nb_operators,
    COUNT(DISTINCT source_name) as nb_sources,
    COUNT(DISTINCT route_name) as nb_routes
FROM trains
GROUP BY train_type;
-- Vue 6: Top opérateurs de nuit
CREATE OR REPLACE VIEW v_top_night_operators AS
SELECT o.agency_name as operator_name,
    o.country,
    t.source_name,
    COUNT(*) as nb_trains,
    COUNT(DISTINCT t.route_name) as nb_routes
FROM trains t
    JOIN operators o ON t.operator_id = o.operator_id
WHERE t.train_type = 'night'
GROUP BY o.agency_name,
    o.country,
    t.source_name
ORDER BY nb_trains DESC
LIMIT 20;