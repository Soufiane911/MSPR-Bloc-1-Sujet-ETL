-- ============================================================
-- SCHÉMA DE BASE DE DONNÉES OBRAIL EUROPE
-- ============================================================
-- Ce script crée la structure complète de la base de données
-- pour le projet ObRail Europe.
-- ============================================================
-- Suppression des tables existantes (pour réinitialisation)
DROP TABLE IF EXISTS schedules CASCADE;
DROP TABLE IF EXISTS trains CASCADE;
DROP TABLE IF EXISTS stations CASCADE;
DROP TABLE IF EXISTS operators CASCADE;
-- ============================================================
-- TABLE: OPERATORS (Opérateurs ferroviaires)
-- ============================================================
CREATE TABLE operators (
    operator_id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    country VARCHAR(50),
    -- Code pays ISO 3166-1 alpha-2, NULL si pan-européen
    website VARCHAR(255),
    source_name VARCHAR(100),
    -- Source des données (SNCF, DB, etc.)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_operator_unique UNIQUE (name, country, source_name)
);
COMMENT ON TABLE operators IS 'Opérateurs ferroviaires européens';
COMMENT ON COLUMN operators.operator_id IS 'Identifiant unique auto-incrémenté';
COMMENT ON COLUMN operators.name IS 'Nom de l''opérateur';
COMMENT ON COLUMN operators.country IS 'Code pays ISO 3166-1 alpha-2 (FR, DE, etc.)';
COMMENT ON COLUMN operators.website IS 'Site web de l''opérateur';
COMMENT ON COLUMN operators.source_name IS 'Source des données (Back-on-Track, SNCF, etc.)';
-- Index sur operators
CREATE INDEX idx_operators_country ON operators(country);
CREATE INDEX idx_operators_source ON operators(source_name);
CREATE INDEX idx_operators_name ON operators(name);
-- ============================================================
-- TABLE: STATIONS (Gares et arrêts)
-- ============================================================
CREATE TABLE stations (
    station_id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(50),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    uic_code VARCHAR(100),
    -- Code UIC ou identifiant station source
    timezone VARCHAR(50) DEFAULT 'Europe/Paris',
    source_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Contraintes
    CONSTRAINT chk_latitude CHECK (
        latitude BETWEEN -90 AND 90
    ),
    CONSTRAINT chk_longitude CHECK (
        longitude BETWEEN -180 AND 180
    ),
    CONSTRAINT uq_station_unique UNIQUE (name, country, source_name)
);
COMMENT ON TABLE stations IS 'Gares et arrêts ferroviaires européens';
COMMENT ON COLUMN stations.station_id IS 'Identifiant unique auto-incrémenté';
COMMENT ON COLUMN stations.name IS 'Nom de la gare';
COMMENT ON COLUMN stations.city IS 'Ville de la gare';
COMMENT ON COLUMN stations.country IS 'Code pays ISO 3166-1 alpha-2';
COMMENT ON COLUMN stations.latitude IS 'Latitude en degrés décimaux';
COMMENT ON COLUMN stations.longitude IS 'Longitude en degrés décimaux';
COMMENT ON COLUMN stations.uic_code IS 'Code UIC international unique par gare';
COMMENT ON COLUMN stations.timezone IS 'Fuseau horaire de la gare';
-- Index sur stations
CREATE INDEX idx_stations_country ON stations(country);
CREATE INDEX idx_stations_city ON stations(city);
CREATE INDEX idx_stations_uic ON stations(uic_code);
CREATE INDEX idx_stations_coordinates ON stations(latitude, longitude);
CREATE INDEX idx_stations_name ON stations(name);
-- ============================================================
-- TABLE: TRAINS (Trains et lignes)
-- ============================================================
CREATE TABLE trains (
    train_id SERIAL PRIMARY KEY,
    train_number VARCHAR(200) NOT NULL,
    operator_id INTEGER NOT NULL REFERENCES operators(operator_id) ON DELETE CASCADE,
    train_type VARCHAR(10) NOT NULL CHECK (train_type IN ('day', 'night')),
    category VARCHAR(50),
    -- TGV, ICE, Eurostar, etc.
    route_name VARCHAR(200),
    -- Nom de la ligne
    train_type_rule VARCHAR(10) CHECK (train_type_rule IN ('day', 'night')),
    train_type_heuristic VARCHAR(10) CHECK (train_type_heuristic IN ('day', 'night')),
    train_type_ml VARCHAR(10) CHECK (train_type_ml IN ('day', 'night')),
    classification_method VARCHAR(50),
    classification_reason VARCHAR(100),
    classification_confidence DECIMAL(4, 2),
    ml_night_probability DECIMAL(4, 2),
    night_percentage DECIMAL(5, 2),
    needs_manual_review BOOLEAN DEFAULT FALSE,
    source_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_train_unique UNIQUE (train_number, operator_id, source_name)
);
COMMENT ON TABLE trains IS 'Trains et lignes ferroviaires';
COMMENT ON COLUMN trains.train_id IS 'Identifiant unique auto-incrémenté';
COMMENT ON COLUMN trains.train_number IS 'Numéro du train';
COMMENT ON COLUMN trains.operator_id IS 'Référence vers l''opérateur';
COMMENT ON COLUMN trains.train_type IS 'Type: day (06h-21h59) ou night (22h-05h59)';
COMMENT ON COLUMN trains.category IS 'Catégorie (TGV, ICE, Eurostar, etc.)';
COMMENT ON COLUMN trains.route_name IS 'Nom de la ligne';
COMMENT ON COLUMN trains.train_type_rule IS 'Classification issue des règles métier fortes';
COMMENT ON COLUMN trains.train_type_heuristic IS 'Classification issue de l''heuristique métier';
COMMENT ON COLUMN trains.train_type_ml IS 'Suggestion issue du modèle ML de support';
COMMENT ON COLUMN trains.classification_method IS 'Méthode finale retenue pour la classification';
COMMENT ON COLUMN trains.classification_reason IS 'Raison principale de la décision finale';
COMMENT ON COLUMN trains.classification_confidence IS 'Niveau de confiance de la classification finale';
COMMENT ON COLUMN trains.ml_night_probability IS 'Probabilité ML associée à la classe night';
COMMENT ON COLUMN trains.night_percentage IS 'Part estimée du trajet effectuée en période nocturne';
COMMENT ON COLUMN trains.needs_manual_review IS 'Indique qu''un contrôle manuel est recommandé';
-- Index sur trains
CREATE INDEX idx_trains_operator ON trains(operator_id);
CREATE INDEX idx_trains_type ON trains(train_type);
CREATE INDEX idx_trains_category ON trains(category);
CREATE INDEX idx_trains_number ON trains(train_number);
-- ============================================================
-- TABLE: SCHEDULES (Dessertes / Horaires)
-- ============================================================
CREATE TABLE schedules (
    schedule_id SERIAL PRIMARY KEY,
    train_id INTEGER NOT NULL REFERENCES trains(train_id) ON DELETE CASCADE,
    origin_id INTEGER NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
    destination_id INTEGER NOT NULL REFERENCES stations(station_id) ON DELETE CASCADE,
    departure_time TIMESTAMP WITH TIME ZONE,
    arrival_time TIMESTAMP WITH TIME ZONE,
    duration_min INTEGER NOT NULL,
    distance_km DECIMAL(10, 2),
    frequency VARCHAR(50),
    -- Quotidien, Week-end, etc.
    source_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Contraintes
    CONSTRAINT chk_different_stations CHECK (origin_id != destination_id),
    CONSTRAINT chk_positive_duration CHECK (duration_min > 0),
    CONSTRAINT uq_schedule_unique UNIQUE (train_id, origin_id, destination_id, departure_time)
);
COMMENT ON TABLE schedules IS 'Dessertes ferroviaires avec horaires';
COMMENT ON COLUMN schedules.schedule_id IS 'Identifiant unique auto-incrémenté';
COMMENT ON COLUMN schedules.train_id IS 'Référence vers le train';
COMMENT ON COLUMN schedules.origin_id IS 'Référence vers la gare de départ';
COMMENT ON COLUMN schedules.destination_id IS 'Référence vers la gare d''arrivée';
COMMENT ON COLUMN schedules.departure_time IS 'Heure de départ (avec fuseau horaire)';
COMMENT ON COLUMN schedules.arrival_time IS 'Heure d''arrivée (avec fuseau horaire)';
COMMENT ON COLUMN schedules.duration_min IS 'Durée du trajet en minutes';
COMMENT ON COLUMN schedules.distance_km IS 'Distance du trajet en kilomètres';
COMMENT ON COLUMN schedules.frequency IS 'Fréquence de service';
-- Index sur schedules
CREATE INDEX idx_schedules_train ON schedules(train_id);
CREATE INDEX idx_schedules_origin ON schedules(origin_id);
CREATE INDEX idx_schedules_destination ON schedules(destination_id);
CREATE INDEX idx_schedules_departure ON schedules(departure_time);
CREATE INDEX idx_schedules_route ON schedules(origin_id, destination_id);
CREATE INDEX idx_schedules_train_type ON trains(train_type) INCLUDE (train_id);
-- ============================================================
-- VUES POUR LES ANALYSES
-- ============================================================
-- Vue: Résumé par opérateur
CREATE OR REPLACE VIEW v_operator_summary AS
SELECT o.operator_id,
    o.name AS operator_name,
    o.country,
    COUNT(DISTINCT t.train_id) AS total_trains,
    COUNT(
        DISTINCT CASE
            WHEN t.train_type = 'day' THEN t.train_id
        END
    ) AS day_trains,
    COUNT(
        DISTINCT CASE
            WHEN t.train_type = 'night' THEN t.train_id
        END
    ) AS night_trains,
    COUNT(s.schedule_id) AS total_schedules,
    ROUND(AVG(s.duration_min), 0) AS avg_duration_min,
    ROUND(AVG(s.distance_km), 0) AS avg_distance_km
FROM operators o
    LEFT JOIN trains t ON o.operator_id = t.operator_id
    LEFT JOIN schedules s ON t.train_id = s.train_id
GROUP BY o.operator_id,
    o.name,
    o.country
ORDER BY total_trains DESC;
COMMENT ON VIEW v_operator_summary IS 'Vue récapitulative des statistiques par opérateur';
-- Vue: Statistiques par pays
CREATE OR REPLACE VIEW v_country_stats AS
SELECT o.country,
    COUNT(DISTINCT o.operator_id) AS nb_operators,
    COUNT(DISTINCT t.train_id) AS nb_trains,
    COUNT(
        DISTINCT CASE
            WHEN t.train_type = 'day' THEN t.train_id
        END
    ) AS day_trains,
    COUNT(
        DISTINCT CASE
            WHEN t.train_type = 'night' THEN t.train_id
        END
    ) AS night_trains,
    COUNT(DISTINCT s.origin_id) AS nb_origin_stations,
    COUNT(DISTINCT s.destination_id) AS nb_destination_stations,
    ROUND(AVG(s.duration_min), 0) AS avg_duration_min,
    ROUND(AVG(s.distance_km), 0) AS avg_distance_km
FROM operators o
    LEFT JOIN trains t ON o.operator_id = t.operator_id
    LEFT JOIN schedules s ON t.train_id = s.train_id
GROUP BY o.country
ORDER BY nb_trains DESC;
COMMENT ON VIEW v_country_stats IS 'Vue des statistiques agrégées par pays';
-- Vue: Dessertes avec noms de gares (pour affichage)
CREATE OR REPLACE VIEW v_schedules_detailed AS
SELECT s.schedule_id,
    t.train_id,
    t.train_number,
    t.train_type,
    t.category,
    o.operator_id,
    o.name AS operator_name,
    o.country AS operator_country,
    so.station_id AS origin_id,
    so.name AS origin_name,
    so.city AS origin_city,
    so.country AS origin_country,
    sd.station_id AS destination_id,
    sd.name AS destination_name,
    sd.city AS destination_city,
    sd.country AS destination_country,
    s.departure_time,
    s.arrival_time,
    s.duration_min,
    s.distance_km,
    s.frequency
FROM schedules s
    JOIN trains t ON s.train_id = t.train_id
    JOIN operators o ON t.operator_id = o.operator_id
    JOIN stations so ON s.origin_id = so.station_id
    JOIN stations sd ON s.destination_id = sd.station_id;
COMMENT ON VIEW v_schedules_detailed IS 'Vue détaillée des dessertes avec noms de gares';
-- Vue: Comparaison Jour/Nuit
CREATE OR REPLACE VIEW v_day_night_comparison AS
SELECT o.country,
    t.train_type,
    COUNT(DISTINCT t.train_id) AS nb_trains,
    COUNT(s.schedule_id) AS nb_schedules,
    ROUND(AVG(s.duration_min), 0) AS avg_duration_min,
    ROUND(AVG(s.distance_km), 0) AS avg_distance_km,
    MIN(s.duration_min) AS min_duration,
    MAX(s.duration_min) AS max_duration
FROM trains t
    JOIN operators o ON t.operator_id = o.operator_id
    LEFT JOIN schedules s ON t.train_id = s.train_id
GROUP BY o.country,
    t.train_type
ORDER BY o.country,
    t.train_type;
COMMENT ON VIEW v_day_night_comparison IS 'Vue comparative des trains de jour et de nuit par pays';
-- Vue: Qualité des données
CREATE OR REPLACE VIEW v_data_quality AS
SELECT 'operators' AS table_name,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (
        WHERE name IS NOT NULL
    ) AS name_complete,
    COUNT(*) FILTER (
        WHERE country IS NOT NULL
    ) AS country_complete,
    COUNT(*) FILTER (
        WHERE website IS NOT NULL
    ) AS website_complete
FROM operators
UNION ALL
SELECT 'stations' AS table_name,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (
        WHERE name IS NOT NULL
    ) AS name_complete,
    COUNT(*) FILTER (
        WHERE latitude IS NOT NULL
            AND longitude IS NOT NULL
    ) AS coordinates_complete,
    COUNT(*) FILTER (
        WHERE uic_code IS NOT NULL
    ) AS uic_complete
FROM stations
UNION ALL
SELECT 'trains' AS table_name,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (
        WHERE train_number IS NOT NULL
    ) AS number_complete,
    COUNT(*) FILTER (
        WHERE train_type IS NOT NULL
    ) AS type_complete,
    COUNT(*) FILTER (
        WHERE category IS NOT NULL
    ) AS category_complete
FROM trains
UNION ALL
SELECT 'schedules' AS table_name,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (
        WHERE departure_time IS NOT NULL
    ) AS departure_complete,
    COUNT(*) FILTER (
        WHERE arrival_time IS NOT NULL
    ) AS arrival_complete,
    COUNT(*) FILTER (
        WHERE distance_km IS NOT NULL
    ) AS distance_complete
FROM schedules;
COMMENT ON VIEW v_data_quality IS 'Vue de la qualité et complétude des données';
-- ============================================================
-- FONCTIONS UTILITAIRES
-- ============================================================
-- Fonction: Classifier un train selon son heure de départ
CREATE OR REPLACE FUNCTION classify_train_type(departure_time TIMESTAMP WITH TIME ZONE) RETURNS VARCHAR(10) AS $$ BEGIN IF EXTRACT(
        HOUR
        FROM departure_time
    ) BETWEEN 6 AND 21 THEN RETURN 'day';
ELSE RETURN 'night';
END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
COMMENT ON FUNCTION classify_train_type IS 'Classifie un train en day ou night selon l''heure de départ';
-- Fonction: Calculer la durée en minutes entre deux timestamps
CREATE OR REPLACE FUNCTION calculate_duration(
        start_time TIMESTAMP WITH TIME ZONE,
        end_time TIMESTAMP WITH TIME ZONE
    ) RETURNS INTEGER AS $$ BEGIN RETURN EXTRACT(
        EPOCH
        FROM (end_time - start_time)
    ) / 60;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
COMMENT ON FUNCTION calculate_duration IS 'Calcule la durée en minutes entre deux timestamps';
-- Fonction: Mettre à jour le timestamp updated_at
CREATE OR REPLACE FUNCTION update_updated_at() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = CURRENT_TIMESTAMP;
RETURN NEW;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION update_updated_at IS 'Met à jour automatiquement le champ updated_at';
-- ============================================================
-- TRIGGERS POUR updated_at
-- ============================================================
CREATE TRIGGER trigger_operators_updated_at BEFORE
UPDATE ON operators FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trigger_stations_updated_at BEFORE
UPDATE ON stations FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trigger_trains_updated_at BEFORE
UPDATE ON trains FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trigger_schedules_updated_at BEFORE
UPDATE ON schedules FOR EACH ROW EXECUTE FUNCTION update_updated_at();
-- ============================================================
-- DONNÉES DE TEST (optionnel)
-- ============================================================
-- Insertion d'opérateurs de test
INSERT INTO operators (name, country, website, source_name)
VALUES (
        'SNCF',
        'FR',
        'https://www.sncf.com',
        'transport.data.gouv'
    ),
    (
        'Deutsche Bahn',
        'DE',
        'https://www.bahn.com',
        'gtfs.de'
    ),
    ('ÖBB', 'AT', 'https://www.oebb.at', 'oebb.at') ON CONFLICT DO NOTHING;
-- Insertion de gares de test
INSERT INTO stations (
        name,
        city,
        country,
        latitude,
        longitude,
        uic_code,
        source_name
    )
VALUES (
        'Paris Gare de Lyon',
        'Paris',
        'FR',
        48.8448,
        2.3735,
        '8768600',
        'transport.data.gouv'
    ),
    (
        'Berlin Hauptbahnhof',
        'Berlin',
        'DE',
        52.5251,
        13.3694,
        '8011160',
        'gtfs.de'
    ),
    (
        'Wien Hauptbahnhof',
        'Vienna',
        'AT',
        48.1853,
        16.3762,
        '8103000',
        'oebb.at'
    ) ON CONFLICT DO NOTHING;
-- ============================================================
-- CONFIRMATION
-- ============================================================
SELECT 'Schéma ObRail Europe créé avec succès' AS status;
