"""GTFS (General Transit Feed Specification) importer for railway data."""

import io
import csv
import zipfile
from datetime import date, time
from typing import Optional, Dict, List
from urllib.request import urlopen, Request
from urllib.error import URLError

from flask import current_app
from sqlalchemy.exc import SQLAlchemyError

from .db import db
from .models import (
    Agence, Ligne, Arret, Trajet, Calendrier, DateCalendrier, HorairePassage
)


class GTFSImporter:
    """Parse and import GTFS feed into database."""

    def __init__(self, gtfsUrl: str):
        self.gtfsUrl = gtfsUrl
        self.zipFile = None
        self.errors = []
        self.stats = {
            "agencies": 0,
            "routes": 0,
            "stops": 0,
            "calendars": 0,
            "trips": 0,
            "stopTimes": 0,
        }

    def importFromFile(self, fileStream) -> bool:
        try:
            current_app.logger.info("Parsing GTFS from uploaded file")
            self.zipFile = zipfile.ZipFile(io.BytesIO(fileStream.read()))
            current_app.logger.info(f"GTFS ZIP parsed, {len(self.zipFile.namelist())} files")
            return self.parseAndImport()
        except zipfile.BadZipFile as exc:
            self.errors.append(f"Invalid ZIP: {str(exc)}")
            current_app.logger.error(f"Bad ZIP: {exc}")
            return False
        except Exception as exc:
            self.errors.append(f"Upload error: {str(exc)}")
            current_app.logger.error(f"Upload error: {exc}")
            return False

    def downloadAndParse(self) -> bool:
        try:
            current_app.logger.info(f"Downloading GTFS from {self.gtfsUrl}")

            req = Request(
                self.gtfsUrl,
                headers={
                    "User-Agent": "Mozilla/5.0 (ObRail-Europe/1.0; +https://github.com/observatoire-mobilite)",
                },
            )
            response = urlopen(req, timeout=30)
            self.zipFile = zipfile.ZipFile(io.BytesIO(response.read()))
            current_app.logger.info(f"GTFS ZIP downloaded, {len(self.zipFile.namelist())} files")
            return self.parseAndImport()
        except URLError as exc:
            self.errors.append(f"Download failed: {str(exc)}")
            current_app.logger.error(f"GTFS download error: {exc}")
            return False
        except zipfile.BadZipFile as exc:
            self.errors.append(f"Invalid ZIP: {str(exc)}")
            current_app.logger.error(f"Bad ZIP: {exc}")
            return False

    def _normalizeTime(self, timeValue: str) -> Optional[time]:
        try:
            parts = timeValue.split(":")
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2]) if len(parts) > 2 else 0

            if hours >= 24:
                hours = hours % 24

            return time(hours, minutes, seconds)
        except (ValueError, IndexError):
            return None

    def _readCsv(self, filename: str) -> Optional[List[Dict]]:
        if self.zipFile is None:
            return None
        try:
            with self.zipFile.open(filename) as gtfsFile:
                reader = csv.DictReader(io.TextIOWrapper(gtfsFile, encoding="utf-8"))
                return list(reader)
        except KeyError:
            current_app.logger.warning(f"File {filename} not found in GTFS")
            return None
        except Exception as exc:
            self.errors.append(f"Error reading {filename}: {str(exc)}")
            return None

    def inspectFeed(self) -> Dict[str, Dict[str, int]]:
        summary = {}
        files = [
            ("agency.txt", "agencies"),
            ("routes.txt", "routes"),
            ("stops.txt", "stops"),
            ("calendar.txt", "calendar"),
            ("calendar_dates.txt", "calendarDates"),
            ("trips.txt", "trips"),
            ("stop_times.txt", "stopTimes"),
        ]

        for filename, key in files:
            rows = self._readCsv(filename)
            if rows is None:
                summary[key] = {"present": 0, "rows": 0}
            else:
                summary[key] = {"present": 1, "rows": len(rows)}

        summary["hasCoreSchedules"] = int(
            summary.get("trips", {}).get("rows", 0) > 0
            and summary.get("stopTimes", {}).get("rows", 0) > 0
        )
        summary["hasCalendar"] = int(
            summary.get("calendar", {}).get("rows", 0) > 0
            or summary.get("calendarDates", {}).get("rows", 0) > 0
        )
        return summary

    def importAgencies(self, rows: List[Dict]) -> Dict[str, int]:
        mapping = {}
        for row in rows:
            try:
                agencyId = row.get("agency_id", "default")
                name = row.get("agency_name", "Unknown")
                url = row.get("agency_url", "")
                timezone = row.get("agency_timezone", "UTC")

                agency = Agence(
                    nomAgence=name[:255],
                    url=url[:255] if url else None,
                    fuseauHoraire=timezone[:50],
                )
                db.session.add(agency)
                db.session.flush()
                mapping[agencyId] = agency.idAgence
                self.stats["agencies"] += 1
            except Exception as exc:
                self.errors.append(f"Agency import error: {str(exc)}")
        return mapping

    def importStops(self, rows: List[Dict]) -> Dict[str, int]:
        mapping = {}
        for row in rows:
            try:
                stopId = row.get("stop_id", "")
                name = row.get("stop_name", "Unknown")
                lat = float(row.get("stop_lat", 0))
                lon = float(row.get("stop_lon", 0))
                zone = row.get("zone_id", None)

                if lat == 0 or lon == 0:
                    current_app.logger.warning(f"Stop {stopId} has invalid coords")
                    continue

                zoneInt = None
                if zone:
                    try:
                        zoneInt = int(zone)
                    except (ValueError, TypeError):
                        zoneInt = None

                arret = Arret(
                    nomArret=name[:255],
                    latitude=lat,
                    longitude=lon,
                    idZone=zoneInt,
                )
                db.session.add(arret)
                db.session.flush()
                mapping[stopId] = arret.idArret
                self.stats["stops"] += 1
            except Exception as exc:
                self.errors.append(f"Stop import error: {str(exc)}")
        return mapping

    def importRoutes(self, rows: List[Dict], agencyMap: Dict) -> Dict[str, int]:
        mapping = {}
        for row in rows:
            try:
                routeId = row.get("route_id", "")
                agencyId = row.get("agency_id", "default")
                nameShort = row.get("route_short_name", "")
                nameLong = row.get("route_long_name", "")
                routeType = row.get("route_type", "3")

                typeMap = {
                    "0": "rail",
                    "1": "subway",
                    "2": "rail",
                    "3": "bus",
                    "4": "ferry",
                }
                typeLigne = typeMap.get(str(routeType), "rail")

                idAgence = agencyMap.get(agencyId)
                if not idAgence:
                    current_app.logger.warning(f"Route {routeId}: no agency found")
                    continue

                ligne = Ligne(
                    idAgence=idAgence,
                    nomCourt=nameShort[:50],
                    nomLong=nameLong[:255],
                    typeLigne=typeLigne,
                )
                db.session.add(ligne)
                db.session.flush()
                mapping[routeId] = ligne.idLigne
                self.stats["routes"] += 1
            except Exception as exc:
                self.errors.append(f"Route import error: {str(exc)}")
        return mapping

    def importCalendars(self, rows: List[Dict]) -> Dict[str, int]:
        mapping = {}
        for row in rows:
            try:
                serviceId = row.get("service_id", "")
                monday = row.get("monday", "0") == "1"
                tuesday = row.get("tuesday", "0") == "1"
                wednesday = row.get("wednesday", "0") == "1"
                thursday = row.get("thursday", "0") == "1"
                friday = row.get("friday", "0") == "1"
                saturday = row.get("saturday", "0") == "1"
                sunday = row.get("sunday", "0") == "1"
                start = row.get("start_date", "20240101")
                end = row.get("end_date", "20251231")

                startDate = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
                endDate = date(int(end[:4]), int(end[4:6]), int(end[6:8]))

                cal = Calendrier(
                    lundi=monday,
                    mardi=tuesday,
                    mercredi=wednesday,
                    jeudi=thursday,
                    vendredi=friday,
                    samedi=saturday,
                    dimanche=sunday,
                    dateDebut=startDate,
                    dateFin=endDate,
                )
                db.session.add(cal)
                db.session.flush()
                mapping[serviceId] = cal.idService
                self.stats["calendars"] += 1
            except Exception as exc:
                self.errors.append(f"Calendar import error: {str(exc)}")
        return mapping

    def importCalendarDates(self, rows: List[Dict], calMap: Dict):
        for row in rows:
            try:
                serviceId = row.get("service_id", "")
                dateValue = row.get("date", "")
                exception = row.get("exception_type", "1")

                idService = calMap.get(serviceId)
                if not idService:
                    continue

                calendarDate = date(int(dateValue[:4]), int(dateValue[4:6]), int(dateValue[6:8]))

                calendarException = DateCalendrier(
                    idService=idService,
                    date=calendarDate,
                    typeException=int(exception),
                )
                db.session.add(calendarException)
            except Exception as exc:
                self.errors.append(f"Calendar date error: {str(exc)}")

    def importTrips(self, rows: List[Dict], routeMap: Dict, calMap: Dict) -> Dict[str, int]:
        mapping = {}
        for row in rows:
            try:
                tripId = row.get("trip_id", "")
                routeId = row.get("route_id", "")
                serviceId = row.get("service_id", "")
                tripHeadsign = row.get("trip_headsign", "Unknown")
                wheelchair = row.get("wheelchair_accessible", "0") == "1"

                isNight = False

                idLigne = routeMap.get(routeId)
                idService = calMap.get(serviceId)
                if not idLigne or not idService:
                    continue

                trajet = Trajet(
                    idLigne=idLigne,
                    idService=idService,
                    destination=tripHeadsign[:255],
                    trainDeNuit=isNight,
                )
                db.session.add(trajet)
                db.session.flush()
                mapping[tripId] = trajet.idTrajet
                self.stats["trips"] += 1
            except Exception as exc:
                self.errors.append(f"Trip import error: {str(exc)}")
        return mapping

    def importStopTimes(self, rows: List[Dict], tripMap: Dict, stopMap: Dict):
        for row in rows:
            try:
                tripId = row.get("trip_id", "")
                stopId = row.get("stop_id", "")
                arrivalTime = row.get("arrival_time", "")
                departureTime = row.get("departure_time", "")
                sequence = row.get("stop_sequence", "0")

                idTrajet = tripMap.get(tripId)
                idArret = stopMap.get(stopId)
                if not idTrajet or not idArret:
                    continue

                arrivalNormalized = None
                departureNormalized = None
                if arrivalTime and len(arrivalTime) >= 5:
                    arrivalNormalized = self._normalizeTime(arrivalTime)
                if departureTime and len(departureTime) >= 5:
                    departureNormalized = self._normalizeTime(departureTime)

                horaire = HorairePassage(
                    idTrajet=idTrajet,
                    idArret=idArret,
                    heureArrivee=arrivalNormalized,
                    heureDepart=departureNormalized,
                    sequenceArret=int(sequence) if sequence else 0,
                )
                db.session.add(horaire)
                self.stats["stopTimes"] += 1
            except Exception as exc:
                self.errors.append(f"Stop time error: {str(exc)}")

    def computeTripStatistics(self):
        from math import radians, sin, cos, sqrt, atan2
        from .models import Trajet, HorairePassage, StatistiquesTrajet

        db.session.query(StatistiquesTrajet).delete()
        db.session.flush()

        def haversine(latOne, lonOne, latTwo, lonTwo):
            radius = 6371
            latDelta = radians(latTwo - latOne)
            lonDelta = radians(lonTwo - lonOne)
            aValue = sin(latDelta / 2) ** 2 + cos(radians(latOne)) * cos(radians(latTwo)) * sin(lonDelta / 2) ** 2
            arc = 2 * atan2(sqrt(aValue), sqrt(1 - aValue))
            return radius * arc

        trajets = db.session.query(Trajet).join(HorairePassage).group_by(Trajet.idTrajet).all()

        for trajet in trajets:
            try:
                horaires = sorted(trajet.horaires, key=lambda horaire: horaire.sequenceArret)
                if len(horaires) < 2:
                    continue

                totalDistance = 0
                for index in range(len(horaires) - 1):
                    firstHoraire = horaires[index]
                    nextHoraire = horaires[index + 1]
                    distance = haversine(
                        float(firstHoraire.arret.latitude),
                        float(firstHoraire.arret.longitude),
                        float(nextHoraire.arret.latitude),
                        float(nextHoraire.arret.longitude),
                    )
                    totalDistance += distance

                firstTime = horaires[0].heureDepart or horaires[0].heureArrivee
                lastTime = horaires[-1].heureArrivee or horaires[-1].heureDepart

                durationMinutes = None
                if firstTime and lastTime:
                    startMinutes = firstTime.hour * 60 + firstTime.minute
                    endMinutes = lastTime.hour * 60 + lastTime.minute
                    durationMinutes = endMinutes - startMinutes
                    if durationMinutes < 0:
                        durationMinutes += 24 * 60

                co2PerKm = 15.0
                co2Total = totalDistance * co2PerKm
                co2PerPassenger = co2Total / 100

                averageSpeed = None
                if durationMinutes and durationMinutes > 0:
                    averageSpeed = (totalDistance / durationMinutes) * 60

                stat = db.session.query(StatistiquesTrajet).filter_by(idTrajet=trajet.idTrajet).first()
                if not stat:
                    stat = StatistiquesTrajet(idTrajet=trajet.idTrajet)
                    db.session.add(stat)

                stat.distanceKm = round(totalDistance, 2)
                stat.dureeMinutes = durationMinutes
                stat.vitesseMoyenneKmh = round(averageSpeed, 1) if averageSpeed else None
                stat.co2TotalG = round(co2Total, 0)
                stat.co2ParPassagerG = round(co2PerPassenger, 1)

            except Exception as exc:
                current_app.logger.warning(f"Stats calculation error for trip {trajet.idTrajet}: {exc}")
                continue

    def parseAndImport(self) -> bool:
        try:
            agencyRows = self._readCsv("agency.txt")
            if agencyRows:
                agencyMap = self.importAgencies(agencyRows)
            else:
                agencyMap = {}

            stopRows = self._readCsv("stops.txt")
            if stopRows:
                stopMap = self.importStops(stopRows)
            else:
                stopMap = {}

            routeRows = self._readCsv("routes.txt")
            if routeRows:
                routeMap = self.importRoutes(routeRows, agencyMap)
            else:
                routeMap = {}

            calendarRows = self._readCsv("calendar.txt")
            if calendarRows:
                calendarMap = self.importCalendars(calendarRows)
            else:
                calendarMap = {}

            calendarDateRows = self._readCsv("calendar_dates.txt")
            if calendarDateRows:
                self.importCalendarDates(calendarDateRows, calendarMap)

            tripRows = self._readCsv("trips.txt")
            if tripRows:
                tripMap = self.importTrips(tripRows, routeMap, calendarMap)
            else:
                tripMap = {}

            stopTimeRows = self._readCsv("stop_times.txt")
            if stopTimeRows:
                self.importStopTimes(stopTimeRows, tripMap, stopMap)

            db.session.commit()

            current_app.logger.info("Computing trip statistics (distance, CO2)...")
            self.computeTripStatistics()
            db.session.commit()

            current_app.logger.info(f"GTFS import successful: {self.stats}")
            return True

        except SQLAlchemyError as exc:
            db.session.rollback()
            self.errors.append(f"Database error: {str(exc)}")
            current_app.logger.exception("GTFS import DB error")
            return False
        except Exception as exc:
            db.session.rollback()
            self.errors.append(f"Unexpected error: {str(exc)}")
            current_app.logger.exception("GTFS import error")
            return False

    def run(self) -> bool:
        if not self.downloadAndParse():
            return False
        return self.parseAndImport()