"""Seed data for development and testing."""

from datetime import date, time
from .db import db
from .models import (
    Agence, TypeVehicule, Ligne, ModeleTrain, Calendrier,
    Forme, Arret, Trajet, HorairePassage, StatistiquesTrajet
)


def seed_dev_data():
    """Populate database with minimal dev data: 2 agencies, 3 lines, 4 trips (day/night mix)."""
    
    # Clear existing data
    db.session.query(Trajet).delete()
    db.session.query(Arret).delete()
    db.session.query(Calendrier).delete()
    db.session.query(Ligne).delete()
    db.session.query(ModeleTrain).delete()
    db.session.query(TypeVehicule).delete()
    db.session.query(Agence).delete()
    db.session.commit()

    # Create agencies
    sncf = Agence(nom_agence="SNCF", url="https://www.sncf.com", fuseau_horaire="Europe/Paris")
    db_ag = Agence(nom_agence="Deutsche Bahn", url="https://www.deutschebahn.com", fuseau_horaire="Europe/Berlin")
    db.session.add_all([sncf, db_ag])
    db.session.flush()

    # Create vehicle types
    tgv_type = TypeVehicule(nom_type_vehicule="TGV", co2_par_km_defaut=14.2, co2_par_pkm_defaut=5.6)
    ice_type = TypeVehicule(nom_type_vehicule="ICE", co2_par_km_defaut=15.0, co2_par_pkm_defaut=5.8)
    db.session.add_all([tgv_type, ice_type])
    db.session.flush()

    # Create train models
    tgv_model = ModeleTrain(
        id_type_vehicule=tgv_type.id_type_vehicule,
        nom_modele="TGV InOui",
        fabricant="Alstom",
        type_energie="electrique",
        nombre_places=915,
        co2_par_km=14.2,
        co2_par_pkm=5.6
    )
    ice_model = ModeleTrain(
        id_type_vehicule=ice_type.id_type_vehicule,
        nom_modele="ICE 4",
        fabricant="Siemens",
        type_energie="electrique",
        nombre_places=830,
        co2_par_km=15.0,
        co2_par_pkm=5.8
    )
    db.session.add_all([tgv_model, ice_model])
    db.session.flush()

    # Create lines
    paris_marseille = Ligne(
        id_agence=sncf.id_agence,
        nom_court="PM",
        nom_long="Paris - Marseille",
        type_ligne="rail",
        id_type_vehicule=tgv_type.id_type_vehicule
    )
    paris_nuit = Ligne(
        id_agence=sncf.id_agence,
        nom_court="PN",
        nom_long="Paris - Nice (Nuit)",
        type_ligne="rail",
        id_type_vehicule=tgv_type.id_type_vehicule
    )
    berlin_munich = Ligne(
        id_agence=db_ag.id_agence,
        nom_court="BM",
        nom_long="Berlin - Munich",
        type_ligne="rail",
        id_type_vehicule=ice_type.id_type_vehicule
    )
    db.session.add_all([paris_marseille, paris_nuit, berlin_munich])
    db.session.flush()

    # Create calendars (services)
    service_daily = Calendrier(
        lundi=True, mardi=True, mercredi=True, jeudi=True,
        vendredi=True, samedi=True, dimanche=True,
        date_debut=date(2024, 1, 1),
        date_fin=date(2025, 12, 31)
    )
    service_night = Calendrier(
        lundi=True, mardi=True, mercredi=True, jeudi=True,
        vendredi=True, samedi=False, dimanche=False,
        date_debut=date(2024, 1, 1),
        date_fin=date(2025, 12, 31)
    )
    db.session.add_all([service_daily, service_night])
    db.session.flush()

    # Create stops
    paris_gld = Arret(nom_arret="Paris Gare de Lyon", latitude=48.8438, longitude=2.3736, id_zone=1)
    marseille_st = Arret(nom_arret="Marseille Saint-Charles", latitude=43.3026, longitude=5.3685, id_zone=1)
    nice_ville = Arret(nom_arret="Nice Ville", latitude=43.2965, longitude=7.2584, id_zone=1)
    berlin_hbf = Arret(nom_arret="Berlin Hauptbahnhof", latitude=52.5256, longitude=13.3686, id_zone=1)
    munich_hbf = Arret(nom_arret="München Hauptbahnhof", latitude=48.1408, longitude=11.5582, id_zone=1)
    db.session.add_all([paris_gld, marseille_st, nice_ville, berlin_hbf, munich_hbf])
    db.session.flush()

    # Create trips
    trajet_pm_day = Trajet(
        id_ligne=paris_marseille.id_ligne,
        id_service=service_daily.id_service,
        destination="Marseille",
        id_modele_train=tgv_model.id_modele_train,
        train_de_nuit=False
    )
    trajet_pn_night = Trajet(
        id_ligne=paris_nuit.id_ligne,
        id_service=service_night.id_service,
        destination="Nice",
        id_modele_train=tgv_model.id_modele_train,
        train_de_nuit=True
    )
    trajet_bm_day = Trajet(
        id_ligne=berlin_munich.id_ligne,
        id_service=service_daily.id_service,
        destination="Munich",
        id_modele_train=ice_model.id_modele_train,
        train_de_nuit=False
    )
    db.session.add_all([trajet_pm_day, trajet_pn_night, trajet_bm_day])
    db.session.flush()

    # Create stop times (horaires_passage)
    # Paris-Marseille day trip
    h1 = HorairePassage(
        id_trajet=trajet_pm_day.id_trajet,
        id_arret=paris_gld.id_arret,
        heure_depart=time(7, 0),
        sequence_arret=1
    )
    h2 = HorairePassage(
        id_trajet=trajet_pm_day.id_trajet,
        id_arret=marseille_st.id_arret,
        heure_arrivee=time(10, 30),
        sequence_arret=2
    )
    # Paris-Nice night trip
    h3 = HorairePassage(
        id_trajet=trajet_pn_night.id_trajet,
        id_arret=paris_gld.id_arret,
        heure_depart=time(21, 0),
        sequence_arret=1
    )
    h4 = HorairePassage(
        id_trajet=trajet_pn_night.id_trajet,
        id_arret=nice_ville.id_arret,
        heure_arrivee=time(9, 30),
        sequence_arret=2
    )
    # Berlin-Munich day trip
    h5 = HorairePassage(
        id_trajet=trajet_bm_day.id_trajet,
        id_arret=berlin_hbf.id_arret,
        heure_depart=time(8, 0),
        sequence_arret=1
    )
    h6 = HorairePassage(
        id_trajet=trajet_bm_day.id_trajet,
        id_arret=munich_hbf.id_arret,
        heure_arrivee=time(13, 30),
        sequence_arret=2
    )
    db.session.add_all([h1, h2, h3, h4, h5, h6])
    db.session.commit()

    # Create stats
    stat1 = StatistiquesTrajet(
        id_trajet=trajet_pm_day.id_trajet,
        distance_km=775,
        duree_minutes=210,
        vitesse_moyenne_kmh=221,
        co2_total_g=11000,
        co2_par_passager_g=12
    )
    stat2 = StatistiquesTrajet(
        id_trajet=trajet_pn_night.id_trajet,
        distance_km=934,
        duree_minutes=600,
        vitesse_moyenne_kmh=93,
        co2_total_g=13254,
        co2_par_passager_g=14.5
    )
    stat3 = StatistiquesTrajet(
        id_trajet=trajet_bm_day.id_trajet,
        distance_km=587,
        duree_minutes=270,
        vitesse_moyenne_kmh=130,
        co2_total_g=8805,
        co2_par_passager_g=10.6
    )
    db.session.add_all([stat1, stat2, stat3])
    db.session.commit()
