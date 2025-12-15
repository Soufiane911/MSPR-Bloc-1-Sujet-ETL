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

    def __init__(self, gtfs_url: str):
        """Initialize importer with GTFS ZIP URL."""
        self.gtfs_url = gtfs_url
        self.zip_file = None
        self.errors = []
        self.stats = {
            "agencies": 0,
            "routes": 0,
            "stops": 0,
            "calendars": 0,
            "trips": 0,
            "stop_times": 0,
        }

    def import_from_file(self, file_stream) -> bool:
        """Import GTFS from uploaded file stream."""
        try:
            current_app.logger.info("Parsing GTFS from uploaded file")
            self.zip_file = zipfile.ZipFile(io.BytesIO(file_stream.read()))
            current_app.logger.info(f"GTFS ZIP parsed, {len(self.zip_file.namelist())} files")
            return self.parse_and_import()
        except zipfile.BadZipFile as e:
            self.errors.append(f"Invalid ZIP: {str(e)}")
            current_app.logger.error(f"Bad ZIP: {e}")
            return False
        except Exception as e:
            self.errors.append(f"Upload error: {str(e)}")
            current_app.logger.error(f"Upload error: {e}")
            return False

    def download_and_parse(self) -> bool:
        """Download GTFS ZIP and parse CSV files."""
        try:
            current_app.logger.info(f"Downloading GTFS from {self.gtfs_url}")
            
            # Create request with User-Agent to avoid 403 Forbidden
            req = Request(
                self.gtfs_url,
                headers={
                    'User-Agent': 'Mozilla/5.0 (ObRail-Europe/1.0; +https://github.com/observatoire-mobilite)'
                }
            )
            response = urlopen(req, timeout=30)
            self.zip_file = zipfile.ZipFile(io.BytesIO(response.read()))
            current_app.logger.info(f"GTFS ZIP downloaded, {len(self.zip_file.namelist())} files")
            return self.parse_and_import()
        except URLError as e:
            self.errors.append(f"Download failed: {str(e)}")
            current_app.logger.error(f"GTFS download error: {e}")
            return False
        except zipfile.BadZipFile as e:
            self.errors.append(f"Invalid ZIP: {str(e)}")
            current_app.logger.error(f"Bad ZIP: {e}")
            return False

    def _normalize_time(self, time_str: str) -> Optional[time]:
        """Normalize GTFS time to Python time object (handles times >= 24:00 for next-day services).
        
        GTFS allows times like 24:01 or 25:30 for services after midnight.
        SQLite and PostgreSQL TIME types require valid time objects (00:00-23:59).
        We convert 24:00+ to modulo 24 (24:01 -> 00:01, 25:30 -> 01:30).
        
        Returns Python time object for database compatibility.
        """
        try:
            parts = time_str.split(":")
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2]) if len(parts) > 2 else 0
            
            # Normalize hours >= 24
            if hours >= 24:
                hours = hours % 24
            
            return time(hours, minutes, seconds)
        except (ValueError, IndexError):
            return None

    def _read_csv(self, filename: str) -> Optional[List[Dict]]:
        """Read CSV from ZIP file and return list of dicts."""
        if self.zip_file is None:
            return None
        try:
            with self.zip_file.open(filename) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                return list(reader)
        except KeyError:
            current_app.logger.warning(f"File {filename} not found in GTFS")
            return None
        except Exception as e:
            self.errors.append(f"Error reading {filename}: {str(e)}")
            return None

    def inspect_feed(self) -> Dict[str, Dict[str, int]]:
        """Inspect GTFS ZIP and return row counts for key files without DB writes.

        Returns a dict: {file: {present: 0/1, rows: int}}
        """
        summary = {}
        files = [
            ("agency.txt", "agencies"),
            ("routes.txt", "routes"),
            ("stops.txt", "stops"),
            ("calendar.txt", "calendar"),
            ("calendar_dates.txt", "calendar_dates"),
            ("trips.txt", "trips"),
            ("stop_times.txt", "stop_times"),
        ]

        for filename, key in files:
            rows = self._read_csv(filename)
            if rows is None:
                summary[key] = {"present": 0, "rows": 0}
            else:
                summary[key] = {"present": 1, "rows": len(rows)}

        # Convenience flags
        summary["has_core_schedules"] = int(
            summary.get("trips", {}).get("rows", 0) > 0 and
            summary.get("stop_times", {}).get("rows", 0) > 0
        )
        summary["has_calendar"] = int(
            summary.get("calendar", {}).get("rows", 0) > 0 or
            summary.get("calendar_dates", {}).get("rows", 0) > 0
        )
        return summary

    def import_agencies(self, rows: List[Dict]) -> Dict[str, int]:
        """Import agencies. Return mapping of agency_id -> Agence.id_agence."""
        mapping = {}
        for row in rows:
            try:
                agency_id = row.get("agency_id", "default")
                name = row.get("agency_name", "Unknown")
                url = row.get("agency_url", "")
                timezone = row.get("agency_timezone", "UTC")

                agency = Agence(
                    nom_agence=name[:255],
                    url=url[:255] if url else None,
                    fuseau_horaire=timezone[:50]
                )
                db.session.add(agency)
                db.session.flush()
                mapping[agency_id] = agency.id_agence
                self.stats["agencies"] += 1
            except Exception as e:
                self.errors.append(f"Agency import error: {str(e)}")
        return mapping

    def import_stops(self, rows: List[Dict]) -> Dict[str, int]:
        """Import stops (arrets). Return mapping of stop_id -> Arret.id_arret."""
        mapping = {}
        for row in rows:
            try:
                stop_id = row.get("stop_id", "")
                name = row.get("stop_name", "Unknown")
                lat = float(row.get("stop_lat", 0))
                lon = float(row.get("stop_lon", 0))
                zone = row.get("zone_id", None)

                # Skip if invalid coords
                if lat == 0 or lon == 0:
                    current_app.logger.warning(f"Stop {stop_id} has invalid coords")
                    continue

                # Handle zone_id: can be int or string
                zone_int = None
                if zone:
                    try:
                        zone_int = int(zone)
                    except (ValueError, TypeError):
                        # zone_id is text (like BART's 'LAKE', 'COLS'), ignore it
                        zone_int = None

                arret = Arret(
                    nom_arret=name[:255],
                    latitude=lat,
                    longitude=lon,
                    id_zone=zone_int
                )
                db.session.add(arret)
                db.session.flush()
                mapping[stop_id] = arret.id_arret
                self.stats["stops"] += 1
            except Exception as e:
                self.errors.append(f"Stop import error: {str(e)}")
        return mapping

    def import_routes(self, rows: List[Dict], agency_map: Dict) -> Dict[str, int]:
        """Import routes (lignes). Return mapping of route_id -> Ligne.id_ligne."""
        mapping = {}
        for row in rows:
            try:
                route_id = row.get("route_id", "")
                agency_id = row.get("agency_id", "default")
                name_short = row.get("route_short_name", "")
                name_long = row.get("route_long_name", "")
                route_type = row.get("route_type", "3")  # 3=Bus, 2=Rail, 1=Subway, etc.

                # Map GTFS route_type to our type_ligne
                type_map = {
                    "0": "rail",     # Light rail
                    "1": "subway",   # Subway
                    "2": "rail",     # Rail
                    "3": "bus",      # Bus
                    "4": "ferry",    # Ferry
                }
                type_ligne = type_map.get(str(route_type), "rail")

                id_agence = agency_map.get(agency_id)
                if not id_agence:
                    current_app.logger.warning(f"Route {route_id}: no agency found")
                    continue

                ligne = Ligne(
                    id_agence=id_agence,
                    nom_court=name_short[:50],
                    nom_long=name_long[:255],
                    type_ligne=type_ligne
                )
                db.session.add(ligne)
                db.session.flush()
                mapping[route_id] = ligne.id_ligne
                self.stats["routes"] += 1
            except Exception as e:
                self.errors.append(f"Route import error: {str(e)}")
        return mapping

    def import_calendars(self, rows: List[Dict]) -> Dict[str, int]:
        """Import service calendars. Return mapping of service_id -> Calendrier.id_service."""
        mapping = {}
        for row in rows:
            try:
                service_id = row.get("service_id", "")
                monday = row.get("monday", "0") == "1"
                tuesday = row.get("tuesday", "0") == "1"
                wednesday = row.get("wednesday", "0") == "1"
                thursday = row.get("thursday", "0") == "1"
                friday = row.get("friday", "0") == "1"
                saturday = row.get("saturday", "0") == "1"
                sunday = row.get("sunday", "0") == "1"
                start = row.get("start_date", "20240101")
                end = row.get("end_date", "20251231")

                # Parse YYYYMMDD format
                start_date = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
                end_date = date(int(end[:4]), int(end[4:6]), int(end[6:8]))

                cal = Calendrier(
                    lundi=monday,
                    mardi=tuesday,
                    mercredi=wednesday,
                    jeudi=thursday,
                    vendredi=friday,
                    samedi=saturday,
                    dimanche=sunday,
                    date_debut=start_date,
                    date_fin=end_date
                )
                db.session.add(cal)
                db.session.flush()
                mapping[service_id] = cal.id_service
                self.stats["calendars"] += 1
            except Exception as e:
                self.errors.append(f"Calendar import error: {str(e)}")
        return mapping

    def import_calendar_dates(self, rows: List[Dict], cal_map: Dict):
        """Import calendar exceptions (calendar_dates.txt)."""
        for row in rows:
            try:
                service_id = row.get("service_id", "")
                date_str = row.get("date", "")
                exception = row.get("exception_type", "1")

                id_service = cal_map.get(service_id)
                if not id_service:
                    continue

                cal_date = date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))

                cal_exc = DateCalendrier(
                    id_service=id_service,
                    date=cal_date,
                    type_exception=int(exception)
                )
                db.session.add(cal_exc)
            except Exception as e:
                self.errors.append(f"Calendar date error: {str(e)}")

    def import_trips(self, rows: List[Dict], route_map: Dict, cal_map: Dict) -> Dict[str, int]:
        """Import trips (trajets). Return mapping of trip_id -> Trajet.id_trajet."""
        mapping = {}
        for row in rows:
            try:
                trip_id = row.get("trip_id", "")
                route_id = row.get("route_id", "")
                service_id = row.get("service_id", "")
                trip_headsign = row.get("trip_headsign", "Unknown")
                wheelchair = row.get("wheelchair_accessible", "0") == "1"

                # Heuristic: if trip departs late evening (22:00+) and arrives early morning, mark as night
                # This will be refined by stop_times
                is_night = False

                id_ligne = route_map.get(route_id)
                id_service = cal_map.get(service_id)
                if not id_ligne or not id_service:
                    continue

                trajet = Trajet(
                    id_ligne=id_ligne,
                    id_service=id_service,
                    destination=trip_headsign[:255],
                    train_de_nuit=is_night
                )
                db.session.add(trajet)
                db.session.flush()
                mapping[trip_id] = trajet.id_trajet
                self.stats["trips"] += 1
            except Exception as e:
                self.errors.append(f"Trip import error: {str(e)}")
        return mapping

    def import_stop_times(self, rows: List[Dict], trip_map: Dict, stop_map: Dict):
        """Import stop times (horaires_passage)."""
        for row in rows:
            try:
                trip_id = row.get("trip_id", "")
                stop_id = row.get("stop_id", "")
                arrival_time = row.get("arrival_time", "")
                departure_time = row.get("departure_time", "")
                sequence = row.get("stop_sequence", "0")

                id_trajet = trip_map.get(trip_id)
                id_arret = stop_map.get(stop_id)
                if not id_trajet or not id_arret:
                    continue

                # Parse HH:MM:SS format and handle times >= 24:00 (next day)
                arr_time = None
                dep_time = None
                if arrival_time and len(arrival_time) >= 5:
                    arr_time = self._normalize_time(arrival_time)
                if departure_time and len(departure_time) >= 5:
                    dep_time = self._normalize_time(departure_time)

                horaire = HorairePassage(
                    id_trajet=id_trajet,
                    id_arret=id_arret,
                    heure_arrivee=arr_time,
                    heure_depart=dep_time,
                    sequence_arret=int(sequence) if sequence else 0
                )
                db.session.add(horaire)
                self.stats["stop_times"] += 1
            except Exception as e:
                self.errors.append(f"Stop time error: {str(e)}")

    def compute_trip_statistics(self):
        """Calculate distance, duration, and CO2 for all trips with stop times."""
        from math import radians, sin, cos, sqrt, atan2
        from .models import Trajet, HorairePassage, StatistiquesTrajet

        # Reset existing stats to avoid unique constraint conflicts on re-imports
        db.session.query(StatistiquesTrajet).delete()
        db.session.flush()
        
        # Haversine formula to calculate distance between two coordinates
        def haversine(lat1, lon1, lat2, lon2):
            R = 6371  # Earth radius in km
            dlat = radians(lat2 - lat1)
            dlon = radians(lon2 - lon1)
            a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
            c = 2 * atan2(sqrt(a), sqrt(1-a))
            return R * c
        
        # Get all trips with horaires
        trajets = db.session.query(Trajet).join(HorairePassage).group_by(Trajet.id_trajet).all()
        
        for trajet in trajets:
            try:
                horaires = sorted(trajet.horaires, key=lambda h: h.sequence_arret)
                if len(horaires) < 2:
                    continue
                
                # Calculate total distance
                total_distance = 0
                for i in range(len(horaires) - 1):
                    h1 = horaires[i]
                    h2 = horaires[i + 1]
                    dist = haversine(
                        float(h1.arret.latitude), float(h1.arret.longitude),
                        float(h2.arret.latitude), float(h2.arret.longitude)
                    )
                    total_distance += dist
                
                # Calculate duration (first departure to last arrival)
                first_time = horaires[0].heure_depart or horaires[0].heure_arrivee
                last_time = horaires[-1].heure_arrivee or horaires[-1].heure_depart
                
                duree_minutes = None
                if first_time and last_time:
                    # Convert to minutes
                    start_minutes = first_time.hour * 60 + first_time.minute
                    end_minutes = last_time.hour * 60 + last_time.minute
                    duree_minutes = end_minutes - start_minutes
                    if duree_minutes < 0:  # crossed midnight
                        duree_minutes += 24 * 60
                
                # Estimate CO2 (default: 15g/km for trains, assumes 100 passengers)
                co2_per_km = 15.0  # grams CO2 per km for the entire vehicle
                co2_total = total_distance * co2_per_km
                co2_per_passenger = co2_total / 100  # assuming 100 passengers
                
                # Calculate average speed
                vitesse_moyenne = None
                if duree_minutes and duree_minutes > 0:
                    vitesse_moyenne = (total_distance / duree_minutes) * 60  # km/h
                
                # Create or update statistics (upsert on id_trajet)
                stat = db.session.query(StatistiquesTrajet).filter_by(id_trajet=trajet.id_trajet).first()
                if not stat:
                    stat = StatistiquesTrajet(id_trajet=trajet.id_trajet)
                    db.session.add(stat)

                stat.distance_km = round(total_distance, 2)
                stat.duree_minutes = duree_minutes
                stat.vitesse_moyenne_kmh = round(vitesse_moyenne, 1) if vitesse_moyenne else None
                stat.co2_total_g = round(co2_total, 0)
                stat.co2_par_passager_g = round(co2_per_passenger, 1)
                
            except Exception as e:
                current_app.logger.warning(f"Stats calculation error for trip {trajet.id_trajet}: {e}")
                continue

    def parse_and_import(self) -> bool:
        """Execute full GTFS import pipeline (after ZIP is loaded)."""
        try:
            # Import in order (dependencies)
            agency_rows = self._read_csv("agency.txt")
            if agency_rows:
                agency_map = self.import_agencies(agency_rows)
            else:
                agency_map = {}

            stop_rows = self._read_csv("stops.txt")
            if stop_rows:
                stop_map = self.import_stops(stop_rows)
            else:
                stop_map = {}

            route_rows = self._read_csv("routes.txt")
            if route_rows:
                route_map = self.import_routes(route_rows, agency_map)
            else:
                route_map = {}

            cal_rows = self._read_csv("calendar.txt")
            if cal_rows:
                cal_map = self.import_calendars(cal_rows)
            else:
                cal_map = {}

            cal_date_rows = self._read_csv("calendar_dates.txt")
            if cal_date_rows:
                self.import_calendar_dates(cal_date_rows, cal_map)

            trip_rows = self._read_csv("trips.txt")
            if trip_rows:
                trip_map = self.import_trips(trip_rows, route_map, cal_map)
            else:
                trip_map = {}

            stop_time_rows = self._read_csv("stop_times.txt")
            if stop_time_rows:
                self.import_stop_times(stop_time_rows, trip_map, stop_map)

            db.session.commit()
            
            # Calculate statistics (distance, CO2) for all trips
            current_app.logger.info("Computing trip statistics (distance, CO2)...")
            self.compute_trip_statistics()
            db.session.commit()
            
            current_app.logger.info(f"GTFS import successful: {self.stats}")
            return True

        except SQLAlchemyError as e:
            db.session.rollback()
            self.errors.append(f"Database error: {str(e)}")
            current_app.logger.exception("GTFS import DB error")
            return False
        except Exception as e:
            db.session.rollback()
            self.errors.append(f"Unexpected error: {str(e)}")
            current_app.logger.exception("GTFS import error")
            return False
    def run(self) -> bool:
        """Execute full GTFS import pipeline from URL."""
        if not self.download_and_parse():
            return False
        return self.parse_and_import()