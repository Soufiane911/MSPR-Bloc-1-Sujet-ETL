from app.db import db


class Agence(db.Model):
    __tablename__ = "agences"

    idAgence = db.Column("id_agence", db.Integer, primary_key=True)
    nomAgence = db.Column("nom_agence", db.Text, nullable=False)
    url = db.Column(db.Text)
    fuseauHoraire = db.Column("fuseau_horaire", db.Text, nullable=False)
    createdAt = db.Column("created_at", db.DateTime(timezone=True), server_default=db.func.now())

    lignes = db.relationship("Ligne", back_populates="agence", cascade="all, delete")


class TypeVehicule(db.Model):
    __tablename__ = "types_vehicules"

    idTypeVehicule = db.Column("id_type_vehicule", db.Integer, primary_key=True)
    nomTypeVehicule = db.Column("nom_type_vehicule", db.Text, nullable=False)
    co2ParKmDefaut = db.Column("co2_par_km_defaut", db.Numeric)
    co2ParPkmDefaut = db.Column("co2_par_pkm_defaut", db.Numeric)

    modeles = db.relationship("ModeleTrain", back_populates="typeVehicule")
    lignes = db.relationship("Ligne", back_populates="typeVehicule")


class Ligne(db.Model):
    __tablename__ = "lignes"

    idLigne = db.Column("id_ligne", db.Integer, primary_key=True)
    idAgence = db.Column("id_agence", db.Integer, db.ForeignKey("agences.id_agence"), nullable=False)
    nomCourt = db.Column("nom_court", db.Text)
    nomLong = db.Column("nom_long", db.Text)
    typeLigne = db.Column("type_ligne", db.Text)
    idTypeVehicule = db.Column("id_type_vehicule", db.Integer, db.ForeignKey("types_vehicules.id_type_vehicule"))

    agence = db.relationship("Agence", back_populates="lignes")
    typeVehicule = db.relationship("TypeVehicule", back_populates="lignes")
    trajets = db.relationship("Trajet", back_populates="ligne", cascade="all, delete")


class ModeleTrain(db.Model):
    __tablename__ = "modeles_trains"

    idModeleTrain = db.Column("id_modele_train", db.Integer, primary_key=True)
    idTypeVehicule = db.Column("id_type_vehicule", db.Integer, db.ForeignKey("types_vehicules.id_type_vehicule"), nullable=False)
    nomModele = db.Column("nom_modele", db.Text)
    fabricant = db.Column(db.Text)
    typeEnergie = db.Column("type_energie", db.Text)
    nombrePlaces = db.Column("nombre_places", db.Integer)
    co2ParKm = db.Column("co2_par_km", db.Numeric)
    co2ParPkm = db.Column("co2_par_pkm", db.Numeric)

    typeVehicule = db.relationship("TypeVehicule", back_populates="modeles")
    trajets = db.relationship("Trajet", back_populates="modeleTrain")


class Calendrier(db.Model):
    __tablename__ = "calendriers"

    idService = db.Column("id_service", db.Integer, primary_key=True)
    lundi = db.Column(db.Boolean)
    mardi = db.Column(db.Boolean)
    mercredi = db.Column(db.Boolean)
    jeudi = db.Column(db.Boolean)
    vendredi = db.Column(db.Boolean)
    samedi = db.Column(db.Boolean)
    dimanche = db.Column(db.Boolean)
    dateDebut = db.Column("date_debut", db.Date, nullable=False)
    dateFin = db.Column("date_fin", db.Date, nullable=False)

    trajets = db.relationship("Trajet", back_populates="service", cascade="all, delete")
    dates = db.relationship("DateCalendrier", back_populates="service", cascade="all, delete")


class DateCalendrier(db.Model):
    __tablename__ = "dates_calendrier"

    idService = db.Column("id_service", db.Integer, db.ForeignKey("calendriers.id_service"), primary_key=True)
    date = db.Column(db.Date, primary_key=True)
    typeException = db.Column("type_exception", db.Integer, nullable=False)

    service = db.relationship("Calendrier", back_populates="dates")


class Forme(db.Model):
    __tablename__ = "formes"

    idForme = db.Column("id_forme", db.Integer, primary_key=True)
    descriptionForme = db.Column("description_forme", db.Text)

    points = db.relationship("PointForme", back_populates="forme", cascade="all, delete")
    trajets = db.relationship("Trajet", back_populates="forme")


class PointForme(db.Model):
    __tablename__ = "points_forme"

    idPointForme = db.Column("id_point_forme", db.Integer, primary_key=True)
    idForme = db.Column("id_forme", db.Integer, db.ForeignKey("formes.id_forme"), nullable=False)
    latitude = db.Column(db.Numeric, nullable=False)
    longitude = db.Column(db.Numeric, nullable=False)
    sequencePoint = db.Column("sequence_point", db.Integer, nullable=False)
    geom = db.Column(db.Text)

    forme = db.relationship("Forme", back_populates="points")


class Trajet(db.Model):
    __tablename__ = "trajets"

    idTrajet = db.Column("id_trajet", db.Integer, primary_key=True)
    idLigne = db.Column("id_ligne", db.Integer, db.ForeignKey("lignes.id_ligne"), nullable=False)
    idService = db.Column("id_service", db.Integer, db.ForeignKey("calendriers.id_service"), nullable=False)
    destination = db.Column(db.Text)
    idForme = db.Column("id_forme", db.Integer, db.ForeignKey("formes.id_forme"))
    idModeleTrain = db.Column("id_modele_train", db.Integer, db.ForeignKey("modeles_trains.id_modele_train"))
    trainDeNuit = db.Column("train_de_nuit", db.Boolean, nullable=False, default=False)

    ligne = db.relationship("Ligne", back_populates="trajets")
    service = db.relationship("Calendrier", back_populates="trajets")
    forme = db.relationship("Forme", back_populates="trajets")
    modeleTrain = db.relationship("ModeleTrain", back_populates="trajets")
    horaires = db.relationship("HorairePassage", back_populates="trajet", cascade="all, delete")
    stats = db.relationship("StatistiquesTrajet", back_populates="trajet", uselist=False, cascade="all, delete")


class Arret(db.Model):
    __tablename__ = "arrets"

    idArret = db.Column("id_arret", db.Integer, primary_key=True)
    nomArret = db.Column("nom_arret", db.Text, nullable=False)
    latitude = db.Column(db.Numeric, nullable=False)
    longitude = db.Column(db.Numeric, nullable=False)
    idZone = db.Column("id_zone", db.Integer)
    geom = db.Column(db.Text)

    horaires = db.relationship("HorairePassage", back_populates="arret", cascade="all, delete")


class HorairePassage(db.Model):
    __tablename__ = "horaires_passage"

    idTrajet = db.Column("id_trajet", db.Integer, db.ForeignKey("trajets.id_trajet"), primary_key=True)
    idArret = db.Column("id_arret", db.Integer, db.ForeignKey("arrets.id_arret"), primary_key=True)
    sequenceArret = db.Column("sequence_arret", db.Integer, primary_key=True, nullable=False)
    heureArrivee = db.Column("heure_arrivee", db.Time)
    heureDepart = db.Column("heure_depart", db.Time)

    trajet = db.relationship("Trajet", back_populates="horaires")
    arret = db.relationship("Arret", back_populates="horaires")


class StatistiquesTrajet(db.Model):
    __tablename__ = "statistiques_trajets"

    idStatistiques = db.Column("id_statistiques", db.Integer, primary_key=True)
    idTrajet = db.Column("id_trajet", db.Integer, db.ForeignKey("trajets.id_trajet"), nullable=False, unique=True)
    distanceKm = db.Column("distance_km", db.Numeric)
    dureeMinutes = db.Column("duree_minutes", db.Integer)
    vitesseMoyenneKmh = db.Column("vitesse_moyenne_kmh", db.Numeric)
    co2TotalG = db.Column("co2_total_g", db.Numeric)
    co2ParPassagerG = db.Column("co2_par_passager_g", db.Numeric)

    trajet = db.relationship("Trajet", back_populates="stats")
