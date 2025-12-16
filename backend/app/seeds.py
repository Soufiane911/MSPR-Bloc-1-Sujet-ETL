from datetime import date, time
from .db import db
from .models import Agence, TypeVehicule, Ligne, ModeleTrain, Calendrier, Forme, Arret, Trajet, HorairePassage, StatistiquesTrajet


def seedDevData():
    db.session.query(Trajet).delete()
    db.session.query(Arret).delete()
    db.session.query(Calendrier).delete()
    db.session.query(Ligne).delete()
    db.session.query(ModeleTrain).delete()
    db.session.query(TypeVehicule).delete()
    db.session.query(Agence).delete()
    db.session.commit()

    sncf = Agence(nomAgence="SNCF", url="https://www.sncf.com", fuseauHoraire="Europe/Paris")
    dbAgence = Agence(nomAgence="Deutsche Bahn", url="https://www.deutschebahn.com", fuseauHoraire="Europe/Berlin")
    db.session.add_all([sncf, dbAgence])
    db.session.flush()

    tgvType = TypeVehicule(nomTypeVehicule="TGV", co2ParKmDefaut=14.2, co2ParPkmDefaut=5.6)
    iceType = TypeVehicule(nomTypeVehicule="ICE", co2ParKmDefaut=15.0, co2ParPkmDefaut=5.8)
    db.session.add_all([tgvType, iceType])
    db.session.flush()

    tgvModel = ModeleTrain(
        idTypeVehicule=tgvType.idTypeVehicule,
        nomModele="TGV InOui",
        fabricant="Alstom",
        typeEnergie="electrique",
        nombrePlaces=915,
        co2ParKm=14.2,
        co2ParPkm=5.6,
    )
    iceModel = ModeleTrain(
        idTypeVehicule=iceType.idTypeVehicule,
        nomModele="ICE 4",
        fabricant="Siemens",
        typeEnergie="electrique",
        nombrePlaces=830,
        co2ParKm=15.0,
        co2ParPkm=5.8,
    )
    db.session.add_all([tgvModel, iceModel])
    db.session.flush()

    parisMarseille = Ligne(idAgence=sncf.idAgence, nomCourt="PM", nomLong="Paris - Marseille", typeLigne="rail", idTypeVehicule=tgvType.idTypeVehicule)
    parisNuit = Ligne(idAgence=sncf.idAgence, nomCourt="PN", nomLong="Paris - Nice (Nuit)", typeLigne="rail", idTypeVehicule=tgvType.idTypeVehicule)
    berlinMunich = Ligne(idAgence=dbAgence.idAgence, nomCourt="BM", nomLong="Berlin - Munich", typeLigne="rail", idTypeVehicule=iceType.idTypeVehicule)
    db.session.add_all([parisMarseille, parisNuit, berlinMunich])
    db.session.flush()

    serviceDaily = Calendrier(lundi=True, mardi=True, mercredi=True, jeudi=True, vendredi=True, samedi=True, dimanche=True, dateDebut=date(2024, 1, 1), dateFin=date(2025, 12, 31))
    serviceNight = Calendrier(lundi=True, mardi=True, mercredi=True, jeudi=True, vendredi=True, samedi=False, dimanche=False, dateDebut=date(2024, 1, 1), dateFin=date(2025, 12, 31))
    db.session.add_all([serviceDaily, serviceNight])
    db.session.flush()

    parisGareDeLyon = Arret(nomArret="Paris Gare de Lyon", latitude=48.8438, longitude=2.3736, idZone=1)
    marseilleSaintCharles = Arret(nomArret="Marseille Saint-Charles", latitude=43.3026, longitude=5.3685, idZone=1)
    niceVille = Arret(nomArret="Nice Ville", latitude=43.2965, longitude=7.2584, idZone=1)
    berlinHbf = Arret(nomArret="Berlin Hauptbahnhof", latitude=52.5256, longitude=13.3686, idZone=1)
    munichHbf = Arret(nomArret="München Hauptbahnhof", latitude=48.1408, longitude=11.5582, idZone=1)
    db.session.add_all([parisGareDeLyon, marseilleSaintCharles, niceVille, berlinHbf, munichHbf])
    db.session.flush()

    trajetParisMarseilleDay = Trajet(idLigne=parisMarseille.idLigne, idService=serviceDaily.idService, destination="Marseille", idModeleTrain=tgvModel.idModeleTrain, trainDeNuit=False)
    trajetParisNiceNight = Trajet(idLigne=parisNuit.idLigne, idService=serviceNight.idService, destination="Nice", idModeleTrain=tgvModel.idModeleTrain, trainDeNuit=True)
    trajetBerlinMunichDay = Trajet(idLigne=berlinMunich.idLigne, idService=serviceDaily.idService, destination="Munich", idModeleTrain=iceModel.idModeleTrain, trainDeNuit=False)
    db.session.add_all([trajetParisMarseilleDay, trajetParisNiceNight, trajetBerlinMunichDay])
    db.session.flush()

    horaireOne = HorairePassage(idTrajet=trajetParisMarseilleDay.idTrajet, idArret=parisGareDeLyon.idArret, heureDepart=time(7, 0), sequenceArret=1)
    horaireTwo = HorairePassage(idTrajet=trajetParisMarseilleDay.idTrajet, idArret=marseilleSaintCharles.idArret, heureArrivee=time(10, 30), sequenceArret=2)
    horaireThree = HorairePassage(idTrajet=trajetParisNiceNight.idTrajet, idArret=parisGareDeLyon.idArret, heureDepart=time(21, 0), sequenceArret=1)
    horaireFour = HorairePassage(idTrajet=trajetParisNiceNight.idTrajet, idArret=niceVille.idArret, heureArrivee=time(9, 30), sequenceArret=2)
    horaireFive = HorairePassage(idTrajet=trajetBerlinMunichDay.idTrajet, idArret=berlinHbf.idArret, heureDepart=time(8, 0), sequenceArret=1)
    horaireSix = HorairePassage(idTrajet=trajetBerlinMunichDay.idTrajet, idArret=munichHbf.idArret, heureArrivee=time(13, 30), sequenceArret=2)
    db.session.add_all([horaireOne, horaireTwo, horaireThree, horaireFour, horaireFive, horaireSix])
    db.session.commit()

    statOne = StatistiquesTrajet(idTrajet=trajetParisMarseilleDay.idTrajet, distanceKm=775, dureeMinutes=210, vitesseMoyenneKmh=221, co2TotalG=11000, co2ParPassagerG=12)
    statTwo = StatistiquesTrajet(idTrajet=trajetParisNiceNight.idTrajet, distanceKm=934, dureeMinutes=600, vitesseMoyenneKmh=93, co2TotalG=13254, co2ParPassagerG=14.5)
    statThree = StatistiquesTrajet(idTrajet=trajetBerlinMunichDay.idTrajet, distanceKm=587, dureeMinutes=270, vitesseMoyenneKmh=130, co2TotalG=8805, co2ParPassagerG=10.6)
    db.session.add_all([statOne, statTwo, statThree])
    db.session.commit()
