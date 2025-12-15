from app.db import db


class Agence(db.Model):
    __tablename__ = "agences"

    id_agence = db.Column(db.Integer, primary_key=True)
    nom_agence = db.Column(db.Text, nullable=False)
    url = db.Column(db.Text)
    fuseau_horaire = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime(timezone=True), server_default=db.func.now())

    lignes = db.relationship("Ligne", back_populates="agence", cascade="all, delete")


class TypeVehicule(db.Model):
    __tablename__ = "types_vehicules"

    id_type_vehicule = db.Column(db.Integer, primary_key=True)
    nom_type_vehicule = db.Column(db.Text, nullable=False)
    co2_par_km_defaut = db.Column(db.Numeric)
    co2_par_pkm_defaut = db.Column(db.Numeric)

    modeles = db.relationship("ModeleTrain", back_populates="type_vehicule")
    lignes = db.relationship("Ligne", back_populates="type_vehicule")


class Ligne(db.Model):
    __tablename__ = "lignes"

    id_ligne = db.Column(db.Integer, primary_key=True)
    id_agence = db.Column(db.Integer, db.ForeignKey("agences.id_agence"), nullable=False)
    nom_court = db.Column(db.Text)
    nom_long = db.Column(db.Text)
    type_ligne = db.Column(db.Text)
    id_type_vehicule = db.Column(db.Integer, db.ForeignKey("types_vehicules.id_type_vehicule"))

    agence = db.relationship("Agence", back_populates="lignes")
    type_vehicule = db.relationship("TypeVehicule", back_populates="lignes")
    trajets = db.relationship("Trajet", back_populates="ligne", cascade="all, delete")


class ModeleTrain(db.Model):
    __tablename__ = "modeles_trains"

    id_modele_train = db.Column(db.Integer, primary_key=True)
    id_type_vehicule = db.Column(db.Integer, db.ForeignKey("types_vehicules.id_type_vehicule"), nullable=False)
    nom_modele = db.Column(db.Text)
    fabricant = db.Column(db.Text)
    type_energie = db.Column(db.Text)
    nombre_places = db.Column(db.Integer)
    co2_par_km = db.Column(db.Numeric)
    co2_par_pkm = db.Column(db.Numeric)

    type_vehicule = db.relationship("TypeVehicule", back_populates="modeles")
    trajets = db.relationship("Trajet", back_populates="modele_train")


class Calendrier(db.Model):
    __tablename__ = "calendriers"

    id_service = db.Column(db.Integer, primary_key=True)
    lundi = db.Column(db.Boolean)
    mardi = db.Column(db.Boolean)
    mercredi = db.Column(db.Boolean)
    jeudi = db.Column(db.Boolean)
    vendredi = db.Column(db.Boolean)
    samedi = db.Column(db.Boolean)
    dimanche = db.Column(db.Boolean)
    date_debut = db.Column(db.Date, nullable=False)
    date_fin = db.Column(db.Date, nullable=False)

    trajets = db.relationship("Trajet", back_populates="service", cascade="all, delete")
    dates = db.relationship("DateCalendrier", back_populates="service", cascade="all, delete")


class DateCalendrier(db.Model):
    __tablename__ = "dates_calendrier"

    id_service = db.Column(db.Integer, db.ForeignKey("calendriers.id_service"), primary_key=True)
    date = db.Column(db.Date, primary_key=True)
    type_exception = db.Column(db.Integer, nullable=False)

    service = db.relationship("Calendrier", back_populates="dates")


class Forme(db.Model):
    __tablename__ = "formes"

    id_forme = db.Column(db.Integer, primary_key=True)
    description_forme = db.Column(db.Text)

    points = db.relationship("PointForme", back_populates="forme", cascade="all, delete")
    trajets = db.relationship("Trajet", back_populates="forme")


class PointForme(db.Model):
    __tablename__ = "points_forme"

    id_point_forme = db.Column(db.Integer, primary_key=True)
    id_forme = db.Column(db.Integer, db.ForeignKey("formes.id_forme"), nullable=False)
    latitude = db.Column(db.Numeric, nullable=False)
    longitude = db.Column(db.Numeric, nullable=False)
    sequence_point = db.Column(db.Integer, nullable=False)
    geom = db.Column(db.Text)

    forme = db.relationship("Forme", back_populates="points")


class Trajet(db.Model):
    __tablename__ = "trajets"

    id_trajet = db.Column(db.Integer, primary_key=True)
    id_ligne = db.Column(db.Integer, db.ForeignKey("lignes.id_ligne"), nullable=False)
    id_service = db.Column(db.Integer, db.ForeignKey("calendriers.id_service"), nullable=False)
    destination = db.Column(db.Text)
    id_forme = db.Column(db.Integer, db.ForeignKey("formes.id_forme"))
    id_modele_train = db.Column(db.Integer, db.ForeignKey("modeles_trains.id_modele_train"))
    train_de_nuit = db.Column(db.Boolean, nullable=False, default=False)

    ligne = db.relationship("Ligne", back_populates="trajets")
    service = db.relationship("Calendrier", back_populates="trajets")
    forme = db.relationship("Forme", back_populates="trajets")
    modele_train = db.relationship("ModeleTrain", back_populates="trajets")
    horaires = db.relationship("HorairePassage", back_populates="trajet", cascade="all, delete")
    stats = db.relationship("StatistiquesTrajet", back_populates="trajet", uselist=False, cascade="all, delete")


class Arret(db.Model):
    __tablename__ = "arrets"

    id_arret = db.Column(db.Integer, primary_key=True)
    nom_arret = db.Column(db.Text, nullable=False)
    latitude = db.Column(db.Numeric, nullable=False)
    longitude = db.Column(db.Numeric, nullable=False)
    id_zone = db.Column(db.Integer)
    geom = db.Column(db.Text)

    horaires = db.relationship("HorairePassage", back_populates="arret", cascade="all, delete")


class HorairePassage(db.Model):
    __tablename__ = "horaires_passage"

    id_trajet = db.Column(db.Integer, db.ForeignKey("trajets.id_trajet"), primary_key=True)
    id_arret = db.Column(db.Integer, db.ForeignKey("arrets.id_arret"), primary_key=True)
    sequence_arret = db.Column(db.Integer, primary_key=True, nullable=False)
    heure_arrivee = db.Column(db.Time)
    heure_depart = db.Column(db.Time)

    trajet = db.relationship("Trajet", back_populates="horaires")
    arret = db.relationship("Arret", back_populates="horaires")


class StatistiquesTrajet(db.Model):
    __tablename__ = "statistiques_trajets"

    id_statistiques = db.Column(db.Integer, primary_key=True)
    id_trajet = db.Column(db.Integer, db.ForeignKey("trajets.id_trajet"), nullable=False, unique=True)
    distance_km = db.Column(db.Numeric)
    duree_minutes = db.Column(db.Integer)
    vitesse_moyenne_kmh = db.Column(db.Numeric)
    co2_total_g = db.Column(db.Numeric)
    co2_par_passager_g = db.Column(db.Numeric)

    trajet = db.relationship("Trajet", back_populates="stats")
