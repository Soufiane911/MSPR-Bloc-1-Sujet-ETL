CREATE TABLE agences (
    id_agence SERIAL PRIMARY KEY,
    nom_agence TEXT NOT NULL,
    url TEXT,
    fuseau_horaire TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE types_vehicules (
    id_type_vehicule SERIAL PRIMARY KEY,
    nom_type_vehicule TEXT NOT NULL,
    co2_par_km_defaut NUMERIC,
    co2_par_pkm_defaut NUMERIC
);

CREATE TABLE modeles_trains (
    id_modele_train SERIAL PRIMARY KEY,
    id_type_vehicule INTEGER NOT NULL REFERENCES types_vehicules(id_type_vehicule),
    nom_modele TEXT,
    fabricant TEXT,
    type_energie TEXT,
    nombre_places INTEGER,
    co2_par_km NUMERIC,
    co2_par_pkm NUMERIC
);

CREATE TABLE lignes (
    id_ligne SERIAL PRIMARY KEY,
    id_agence INTEGER NOT NULL REFERENCES agences(id_agence),
    nom_court TEXT,
    nom_long TEXT,
    type_ligne TEXT,
    id_type_vehicule INTEGER REFERENCES types_vehicules(id_type_vehicule)
);

CREATE TABLE calendriers (
    id_service SERIAL PRIMARY KEY,
    lundi BOOLEAN,
    mardi BOOLEAN,
    mercredi BOOLEAN,
    jeudi BOOLEAN,
    vendredi BOOLEAN,
    samedi BOOLEAN,
    dimanche BOOLEAN,
    date_debut DATE NOT NULL,
    date_fin DATE NOT NULL
);

CREATE TABLE dates_calendrier (
    id_service INTEGER NOT NULL REFERENCES calendriers(id_service),
    date DATE NOT NULL,
    type_exception INTEGER NOT NULL,
    PRIMARY KEY (id_service, date)
);

CREATE TABLE formes (
    id_forme SERIAL PRIMARY KEY,
    description_forme TEXT
);

CREATE TABLE points_forme (
    id_point_forme SERIAL PRIMARY KEY,
    id_forme INTEGER NOT NULL REFERENCES formes(id_forme),
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,
    sequence_point INTEGER NOT NULL,
    geom TEXT
);

CREATE TABLE arrets (
    id_arret SERIAL PRIMARY KEY,
    nom_arret TEXT NOT NULL,
    latitude NUMERIC NOT NULL,
    longitude NUMERIC NOT NULL,
    id_zone INTEGER,
    geom TEXT
);

CREATE TABLE trajets (
    id_trajet SERIAL PRIMARY KEY,
    id_ligne INTEGER NOT NULL REFERENCES lignes(id_ligne),
    id_service INTEGER NOT NULL REFERENCES calendriers(id_service),
    destination TEXT,
    id_forme INTEGER REFERENCES formes(id_forme),
    id_modele_train INTEGER REFERENCES modeles_trains(id_modele_train),
    train_de_nuit BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE horaires_passage (
    id_trajet INTEGER NOT NULL REFERENCES trajets(id_trajet),
    id_arret INTEGER NOT NULL REFERENCES arrets(id_arret),
    sequence_arret INTEGER NOT NULL,
    heure_arrivee TIME,
    heure_depart TIME,
    PRIMARY KEY (id_trajet, id_arret, sequence_arret)
);

CREATE TABLE statistiques_trajets (
    id_statistiques SERIAL PRIMARY KEY,
    id_trajet INTEGER NOT NULL UNIQUE REFERENCES trajets(id_trajet),
    distance_km NUMERIC,
    duree_minutes INTEGER,
    vitesse_moyenne_kmh NUMERIC,
    co2_total_g NUMERIC,
    co2_par_passager_g NUMERIC
);

CREATE INDEX idx_lignes_agence ON lignes(id_agence);
CREATE INDEX idx_lignes_type_vehicule ON lignes(id_type_vehicule);
CREATE INDEX idx_modeles_trains_type_vehicule ON modeles_trains(id_type_vehicule);
CREATE INDEX idx_trajets_ligne ON trajets(id_ligne);
CREATE INDEX idx_trajets_service ON trajets(id_service);
CREATE INDEX idx_trajets_forme ON trajets(id_forme);
CREATE INDEX idx_trajets_modele ON trajets(id_modele_train);
CREATE INDEX idx_trajets_nuit ON trajets(train_de_nuit);
CREATE INDEX idx_horaires_passage_trajet ON horaires_passage(id_trajet);
CREATE INDEX idx_horaires_passage_arret ON horaires_passage(id_arret);
CREATE INDEX idx_points_forme_forme ON points_forme(id_forme);
CREATE INDEX idx_arrets_zone ON arrets(id_zone);
CREATE INDEX idx_statistiques_trajet ON statistiques_trajets(id_trajet);
