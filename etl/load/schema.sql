-- ============================================================================
-- Schéma de Base de Données - ObRail Europe
-- ============================================================================
-- Base de données : obrail_europe
-- SGBD : PostgreSQL
-- Date : 2025
-- ============================================================================

-- Créer la base de données (à exécuter manuellement si nécessaire)
-- CREATE DATABASE obrail_europe;

-- Se connecter à la base
-- \c obrail_europe;

-- ============================================================================
-- TABLES GTFS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: agency
-- Description: Opérateurs de transport
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agency (
    agency_id VARCHAR(100) PRIMARY KEY NOT NULL,
    agency_name VARCHAR(255) NOT NULL,
    agency_url VARCHAR(500),
    agency_timezone VARCHAR(50) NOT NULL,
    agency_lang VARCHAR(10)
);

COMMENT ON TABLE agency IS 'Opérateurs de transport (SNCF, etc.)';
COMMENT ON COLUMN agency.agency_id IS 'Identifiant unique de l''opérateur';
COMMENT ON COLUMN agency.agency_name IS 'Nom de l''opérateur';
COMMENT ON COLUMN agency.agency_timezone IS 'Fuseau horaire (ex: Europe/Paris)';

-- ----------------------------------------------------------------------------
-- Table: routes
-- Description: Lignes de train
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS routes (
    route_id VARCHAR(100) PRIMARY KEY NOT NULL,
    agency_id VARCHAR(100) NOT NULL,
    route_short_name VARCHAR(50),
    route_long_name VARCHAR(255),
    route_type INTEGER NOT NULL,
    route_color VARCHAR(6),
    route_text_color VARCHAR(6),
    CONSTRAINT fk_routes_agency 
        FOREIGN KEY (agency_id) 
        REFERENCES agency(agency_id) 
        ON DELETE CASCADE
);

COMMENT ON TABLE routes IS 'Lignes de transport (ex: Paris - Lyon)';
COMMENT ON COLUMN routes.route_type IS 'Type de transport (2=train, 3=bus)';

-- Index pour routes
CREATE INDEX IF NOT EXISTS idx_routes_agency_id ON routes(agency_id);

-- ----------------------------------------------------------------------------
-- Table: stops
-- Description: Gares et arrêts
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stops (
    stop_id VARCHAR(100) PRIMARY KEY NOT NULL,
    stop_name VARCHAR(255) NOT NULL,
    stop_lat DECIMAL(10, 7) NOT NULL,
    stop_lon DECIMAL(10, 7) NOT NULL,
    location_type INTEGER NOT NULL,
    parent_station VARCHAR(100),
    CONSTRAINT fk_stops_parent 
        FOREIGN KEY (parent_station) 
        REFERENCES stops(stop_id) 
        ON DELETE SET NULL,
    CONSTRAINT chk_location_type 
        CHECK (location_type IN (0, 1))
);

COMMENT ON TABLE stops IS 'Points d''arrêt (gares, stations)';
COMMENT ON COLUMN stops.location_type IS '0=arrêt, 1=zone d''arrêt';
COMMENT ON COLUMN stops.parent_station IS 'Gare parente (si arrêt, NULL pour StopArea)';

-- Index pour stops
CREATE INDEX IF NOT EXISTS idx_stops_parent_station ON stops(parent_station);
CREATE INDEX IF NOT EXISTS idx_stops_location ON stops(stop_lat, stop_lon);

-- ----------------------------------------------------------------------------
-- Table: calendar_dates
-- Description: Jours de service
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS calendar_dates (
    service_id INTEGER NOT NULL,
    date DATE NOT NULL,
    exception_type INTEGER NOT NULL,
    PRIMARY KEY (service_id, date),
    CONSTRAINT chk_exception_type 
        CHECK (exception_type IN (1, 2))
);

COMMENT ON TABLE calendar_dates IS 'Jours où un service (trajet) est actif';
COMMENT ON COLUMN calendar_dates.exception_type IS '1=ajouté, 2=retiré';

-- Index pour calendar_dates
CREATE INDEX IF NOT EXISTS idx_calendar_dates_service_id ON calendar_dates(service_id);
CREATE INDEX IF NOT EXISTS idx_calendar_dates_date ON calendar_dates(date);

-- ----------------------------------------------------------------------------
-- Table: trips
-- Description: Trajets/voyages
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trips (
    trip_id VARCHAR(200) PRIMARY KEY NOT NULL,
    route_id VARCHAR(100) NOT NULL,
    service_id INTEGER NOT NULL,
    trip_headsign VARCHAR(255),
    direction_id INTEGER,
    block_id INTEGER,
    CONSTRAINT fk_trips_route 
        FOREIGN KEY (route_id) 
        REFERENCES routes(route_id) 
        ON DELETE CASCADE
    -- Note: service_id référence calendar_dates mais pas de FK car clé composite
    -- La relation est logique mais pas contrainte par FK
);

COMMENT ON TABLE trips IS 'Trajets spécifiques sur une ligne (ex: Train 105342)';
COMMENT ON COLUMN trips.direction_id IS 'Direction (0 ou 1, peut être NULL)';

-- Index pour trips
CREATE INDEX IF NOT EXISTS idx_trips_route_id ON trips(route_id);
CREATE INDEX IF NOT EXISTS idx_trips_service_id ON trips(service_id);

-- ----------------------------------------------------------------------------
-- Table: stop_times
-- Description: Horaires par arrêt
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stop_times (
    trip_id VARCHAR(200) NOT NULL,
    arrival_time VARCHAR(10) NOT NULL,  -- Format HH:MM:SS (peut être > 24h pour GTFS)
    departure_time VARCHAR(10) NOT NULL,  -- Format HH:MM:SS (peut être > 24h pour GTFS)
    stop_id VARCHAR(100) NOT NULL,
    stop_sequence INTEGER NOT NULL,
    pickup_type INTEGER NOT NULL,
    drop_off_type INTEGER NOT NULL,
    PRIMARY KEY (trip_id, stop_sequence),
    CONSTRAINT fk_stop_times_trip 
        FOREIGN KEY (trip_id) 
        REFERENCES trips(trip_id) 
        ON DELETE CASCADE,
    CONSTRAINT fk_stop_times_stop 
        FOREIGN KEY (stop_id) 
        REFERENCES stops(stop_id) 
        ON DELETE CASCADE,
    CONSTRAINT chk_pickup_type 
        CHECK (pickup_type IN (0, 1)),
    CONSTRAINT chk_drop_off_type 
        CHECK (drop_off_type IN (0, 1))
);

COMMENT ON TABLE stop_times IS 'Horaires d''arrivée et de départ pour chaque arrêt d''un trajet';
COMMENT ON COLUMN stop_times.pickup_type IS '0=ramassage, 1=pas de ramassage';
COMMENT ON COLUMN stop_times.drop_off_type IS '0=dépose, 1=pas de dépose';

-- Index pour stop_times
CREATE INDEX IF NOT EXISTS idx_stop_times_trip_id ON stop_times(trip_id);
CREATE INDEX IF NOT EXISTS idx_stop_times_stop_id ON stop_times(stop_id);
CREATE INDEX IF NOT EXISTS idx_stop_times_trip_stop ON stop_times(trip_id, stop_id);

-- ============================================================================
-- TABLES BACK-ON-TRACK (Trains de nuit)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: night_trains
-- Description: Trains de nuit européens
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS night_trains (
    routeid VARCHAR(50) PRIMARY KEY NOT NULL,
    nighttrain VARCHAR(255) NOT NULL,
    routelongname VARCHAR(255),
    itinerary VARCHAR(255),
    itinerarylong TEXT,
    countries VARCHAR(255),
    operators VARCHAR(255),
    source VARCHAR(500),
    num_stations_direction_1 INTEGER,
    num_stations_direction_2 INTEGER,
    num_countries INTEGER,
    num_operators INTEGER
);

COMMENT ON TABLE night_trains IS 'Informations sur les trains de nuit européens';
COMMENT ON COLUMN night_trains.routeid IS 'Identifiant unique du train';
COMMENT ON COLUMN night_trains.itinerary IS 'Itinéraire court (ex: București – Arad)';

-- Index pour night_trains
CREATE INDEX IF NOT EXISTS idx_night_trains_countries ON night_trains(countries);
CREATE INDEX IF NOT EXISTS idx_night_trains_operators ON night_trains(operators);

-- ----------------------------------------------------------------------------
-- Table: night_train_countries
-- Description: Relations trains ↔ pays (many-to-many)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS night_train_countries (
    routeid VARCHAR(50) NOT NULL,
    country VARCHAR(10) NOT NULL,
    PRIMARY KEY (routeid, country),
    CONSTRAINT fk_night_train_countries_train 
        FOREIGN KEY (routeid) 
        REFERENCES night_trains(routeid) 
        ON DELETE CASCADE
);

COMMENT ON TABLE night_train_countries IS 'Table de liaison trains ↔ pays (un train peut traverser plusieurs pays)';
COMMENT ON COLUMN night_train_countries.country IS 'Code pays (ex: FR, DE)';

-- Index pour night_train_countries
CREATE INDEX IF NOT EXISTS idx_night_train_countries_routeid ON night_train_countries(routeid);
CREATE INDEX IF NOT EXISTS idx_night_train_countries_country ON night_train_countries(country);

-- ----------------------------------------------------------------------------
-- Table: night_train_operators
-- Description: Relations trains ↔ opérateurs (many-to-many)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS night_train_operators (
    routeid VARCHAR(50) NOT NULL,
    operator VARCHAR(100) NOT NULL,
    PRIMARY KEY (routeid, operator),
    CONSTRAINT fk_night_train_operators_train 
        FOREIGN KEY (routeid) 
        REFERENCES night_trains(routeid) 
        ON DELETE CASCADE
);

COMMENT ON TABLE night_train_operators IS 'Table de liaison trains ↔ opérateurs (un train peut avoir plusieurs opérateurs)';

-- Index pour night_train_operators
CREATE INDEX IF NOT EXISTS idx_night_train_operators_routeid ON night_train_operators(routeid);
CREATE INDEX IF NOT EXISTS idx_night_train_operators_operator ON night_train_operators(operator);

-- ----------------------------------------------------------------------------
-- Table: night_train_stations
-- Description: Gares par direction
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS night_train_stations (
    routeid VARCHAR(50) NOT NULL,
    direction INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    station VARCHAR(255) NOT NULL,
    PRIMARY KEY (routeid, direction, sequence),
    CONSTRAINT fk_night_train_stations_train 
        FOREIGN KEY (routeid) 
        REFERENCES night_trains(routeid) 
        ON DELETE CASCADE,
    CONSTRAINT chk_direction 
        CHECK (direction IN (1, 2))
);

COMMENT ON TABLE night_train_stations IS 'Liste des gares pour chaque direction d''un train de nuit';
COMMENT ON COLUMN night_train_stations.direction IS 'Direction (1 ou 2)';
COMMENT ON COLUMN night_train_stations.sequence IS 'Ordre dans l''itinéraire';

-- Index pour night_train_stations
CREATE INDEX IF NOT EXISTS idx_night_train_stations_routeid ON night_train_stations(routeid);
CREATE INDEX IF NOT EXISTS idx_night_train_stations_route_dir ON night_train_stations(routeid, direction);

-- ============================================================================
-- VUES UTILES (optionnel, pour faciliter les requêtes)
-- ============================================================================

-- Vue: routes avec nom d'agency
CREATE OR REPLACE VIEW v_routes_with_agency AS
SELECT 
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    a.agency_name,
    a.agency_id
FROM routes r
JOIN agency a ON r.agency_id = a.agency_id;

-- Vue: trips avec informations de route
CREATE OR REPLACE VIEW v_trips_with_route AS
SELECT 
    t.trip_id,
    t.trip_headsign,
    t.direction_id,
    r.route_short_name,
    r.route_long_name,
    a.agency_name
FROM trips t
JOIN routes r ON t.route_id = r.route_id
JOIN agency a ON r.agency_id = a.agency_id;

-- ============================================================================
-- STATISTIQUES
-- ============================================================================

-- Analyser les tables pour optimiser les requêtes
ANALYZE agency;
ANALYZE routes;
ANALYZE stops;
ANALYZE trips;
ANALYZE stop_times;
ANALYZE calendar_dates;
ANALYZE night_trains;
ANALYZE night_train_countries;
ANALYZE night_train_operators;
ANALYZE night_train_stations;

-- ============================================================================
-- FIN DU SCRIPT
-- ============================================================================

