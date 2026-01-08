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

    def __init__(self, gtfsUrl: str, progressCallback=None):
        self.gtfsUrl = gtfsUrl
        self.zipFile = None
        self.errors = []
        self.progressCallback = progressCallback
        self.stats = {
            "agencies": 0,
            "routes": 0,
            "stops": 0,
            "calendars": 0,
            "trips": 0,
            "stopTimes": 0,
        }

    def _emitProgress(self, message: str, data: Dict = None):
        if self.progressCallback:
            self.progressCallback(message, data or {})

    def _cleanText(self, text: str, maxLength: int = None) -> Optional[str]:
        """Clean text field: strip whitespace, convert empty to None, truncate."""
        if not text:
            return None
        cleaned = text.strip()
        if not cleaned:
            return None
        if maxLength and len(cleaned) > maxLength:
            cleaned = cleaned[:maxLength]
        return cleaned

    def _validateCoordinate(self, lat: float, lon: float) -> bool:
        """Validate latitude and longitude are within valid ranges."""
        return -90 <= lat <= 90 and -180 <= lon <= 180

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
            self._emitProgress("downloading", {"url": self.gtfsUrl})
            current_app.logger.info(f"Downloading GTFS from {self.gtfsUrl}")

            req = Request(
                self.gtfsUrl,
                headers={
                    "User-Agent": "Mozilla/5.0 (ObRail-Europe/1.0; +https://github.com/observatoire-mobilite)",
                },
            )
            response = urlopen(req, timeout=30)
            self.zipFile = zipfile.ZipFile(io.BytesIO(response.read()))
            self._emitProgress("downloaded", {"files": len(self.zipFile.namelist())})
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
        self._emitProgress("processing", {"file": "agency.txt", "rows": len(rows)})
        mapping = {}
        for row in rows:
            try:
                agencyId = row.get("agency_id", "default").strip()
                name = self._cleanText(row.get("agency_name"), 255) or "Unknown"
                url = self._cleanText(row.get("agency_url"), 255)
                timezone = self._cleanText(row.get("agency_timezone"), 50) or "UTC"

                existing = db.session.query(Agence).filter(Agence.nomAgence.ilike(name)).first()
                if existing:
                    mapping[agencyId] = existing.idAgence
                    continue

                agency = Agence(
                    nomAgence=name,
                    url=url,
                    fuseauHoraire=timezone,
                )
                db.session.add(agency)
                db.session.flush()
                mapping[agencyId] = agency.idAgence
                self.stats["agencies"] += 1
            except Exception as exc:
                self.errors.append(f"Agency import error: {str(exc)}")
        self._emitProgress("completed", {"file": "agency.txt", "imported": self.stats["agencies"]})
        return mapping

    def importStops(self, rows: List[Dict]) -> Dict[str, int]:
        self._emitProgress("processing", {"file": "stops.txt", "rows": len(rows)})
        mapping = {}
        for row in rows:
            try:
                stopId = row.get("stop_id", "").strip()
                name = self._cleanText(row.get("stop_name"), 255) or "Unknown"
                
                try:
                    lat = float(row.get("stop_lat", 0))
                    lon = float(row.get("stop_lon", 0))
                except (ValueError, TypeError):
                    current_app.logger.warning(f"Stop {stopId} has invalid coordinate format")
                    continue

                if lat == 0 or lon == 0 or not self._validateCoordinate(lat, lon):
                    current_app.logger.warning(f"Stop {stopId} has invalid coords: ({lat}, {lon})")
                    continue

                zone = self._cleanText(row.get("zone_id"))
                zoneInt = None
                if zone:
                    try:
                        zoneInt = int(zone)
                    except (ValueError, TypeError):
                        zoneInt = None

                existing = db.session.query(Arret).filter(
                    Arret.nomArret.ilike(name),
                    Arret.latitude.between(lat - 0.0005, lat + 0.0005),
                    Arret.longitude.between(lon - 0.0005, lon + 0.0005)
                ).first()
                if existing:
                    mapping[stopId] = existing.idArret
                    continue

                arret = Arret(
                    nomArret=name,
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
        self._emitProgress("completed", {"file": "stops.txt", "imported": self.stats["stops"]})
        return mapping

    def importRoutes(self, rows: List[Dict], agencyMap: Dict) -> Dict[str, int]:
        self._emitProgress("processing", {"file": "routes.txt", "rows": len(rows)})
        mapping = {}
        for row in rows:
            try:
                routeId = row.get("route_id", "").strip()
                agencyId = row.get("agency_id", "default").strip()
                nameShort = self._cleanText(row.get("route_short_name"), 50)
                nameLong = self._cleanText(row.get("route_long_name"), 255)
                routeType = row.get("route_type", "3").strip()

                if not nameShort and not nameLong:
                    current_app.logger.warning(f"Route {routeId}: missing name")
                    continue

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
                    nomCourt=nameShort or nameLong[:50],
                    nomLong=nameLong or nameShort,
                    typeLigne=typeLigne,
                )
                db.session.add(ligne)
                db.session.flush()
                mapping[routeId] = ligne.idLigne
                self.stats["routes"] += 1
            except Exception as exc:
                self.errors.append(f"Route import error: {str(exc)}")
        self._emitProgress("completed", {"file": "routes.txt", "imported": self.stats["routes"]})
        return mapping

    def importCalendars(self, rows: List[Dict]) -> Dict[str, int]:
        mapping = {}
        for row in rows:
            try:
                serviceId = row.get("service_id", "").strip()
                monday = row.get("monday", "0").strip() == "1"
                tuesday = row.get("tuesday", "0").strip() == "1"
                wednesday = row.get("wednesday", "0").strip() == "1"
                thursday = row.get("thursday", "0").strip() == "1"
                friday = row.get("friday", "0").strip() == "1"
                saturday = row.get("saturday", "0").strip() == "1"
                sunday = row.get("sunday", "0").strip() == "1"
                start = row.get("start_date", "20240101").strip()
                end = row.get("end_date", "20251231").strip()

                try:
                    startDate = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
                    endDate = date(int(end[:4]), int(end[4:6]), int(end[6:8]))
                    
                    if startDate > endDate:
                        current_app.logger.warning(f"Calendar {serviceId}: start date after end date")
                        continue
                    
                    if startDate.year < 1900 or endDate.year > 2100:
                        current_app.logger.warning(f"Calendar {serviceId}: date out of reasonable range")
                        continue
                except (ValueError, IndexError) as e:
                    current_app.logger.warning(f"Calendar {serviceId}: invalid date format - {e}")
                    continue

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
        self._emitProgress("processing", {"file": "trips.txt", "rows": len(rows)})
        mapping = {}
        for row in rows:
            try:
                tripId = row.get("trip_id", "").strip()
                routeId = row.get("route_id", "").strip()
                serviceId = row.get("service_id", "").strip()
                tripHeadsign = self._cleanText(row.get("trip_headsign"), 255) or "Unknown"
                wheelchair = row.get("wheelchair_accessible", "0").strip() == "1"

                isNight = False

                idLigne = routeMap.get(routeId)
                idService = calMap.get(serviceId)
                if not idLigne or not idService:
                    continue

                trajet = Trajet(
                    idLigne=idLigne,
                    idService=idService,
                    destination=tripHeadsign,
                    trainDeNuit=isNight,
                )
                db.session.add(trajet)
                db.session.flush()
                mapping[tripId] = trajet.idTrajet
                self.stats["trips"] += 1
            except Exception as exc:
                self.errors.append(f"Trip import error: {str(exc)}")
        self._emitProgress("completed", {"file": "trips.txt", "imported": self.stats["trips"]})
        return mapping

    def importStopTimes(self, rows: List[Dict], tripMap: Dict, stopMap: Dict):
        self._emitProgress("processing", {"file": "stop_times.txt", "rows": len(rows)})
        for row in rows:
            try:
                tripId = row.get("trip_id", "").strip()
                stopId = row.get("stop_id", "").strip()
                arrivalTime = self._cleanText(row.get("arrival_time"))
                departureTime = self._cleanText(row.get("departure_time"))
                sequence = row.get("stop_sequence", "0").strip()

                idTrajet = tripMap.get(tripId)
                idArret = stopMap.get(stopId)
                if not idTrajet or not idArret:
                    continue

                try:
                    sequenceInt = int(sequence) if sequence else 0
                    if sequenceInt < 0:
                        current_app.logger.warning(f"Stop time: negative sequence {sequenceInt}")
                        continue
                except (ValueError, TypeError):
                    current_app.logger.warning(f"Stop time: invalid sequence {sequence}")
                    continue

                arrivalNormalized = None
                departureNormalized = None
                if arrivalTime and len(arrivalTime) >= 5:
                    arrivalNormalized = self._normalizeTime(arrivalTime)
                if departureTime and len(departureTime) >= 5:
                    departureNormalized = self._normalizeTime(departureTime)

                if not arrivalNormalized and not departureNormalized:
                    continue

                horaire = HorairePassage(
                    idTrajet=idTrajet,
                    idArret=idArret,
                    heureArrivee=arrivalNormalized,
                    heureDepart=departureNormalized,
                    sequenceArret=sequenceInt,
                )
                db.session.add(horaire)
                self.stats["stopTimes"] += 1
            except Exception as exc:
                self.errors.append(f"Stop time error: {str(exc)}")
        self._emitProgress("completed", {"file": "stop_times.txt", "imported": self.stats["stopTimes"]})

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

            self._emitProgress("computing_stats", {"message": "Computing trip statistics..."})
            current_app.logger.info("Computing trip statistics (distance, CO2)...")
            self.computeTripStatistics()
            db.session.commit()

            self._emitProgress("complete", {"stats": self.stats})
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