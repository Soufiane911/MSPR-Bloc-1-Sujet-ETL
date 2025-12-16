#!/usr/bin/env python3
"""Create a minimal GTFS ZIP file for testing."""

import csv
import zipfile
from io import StringIO
from pathlib import Path

def _writeCsv(data):
    output = StringIO()
    writer = csv.writer(output)
    for row in data:
        writer.writerow(row)
    return output.getvalue()

gtfsDir = Path(__file__).parent
zipPath = gtfsDir / "test_gtfs.zip"

agencyData = [
    ["agency_id", "agency_name", "agency_url", "agency_timezone"],
    ["1", "SNCF Test", "https://sncf.com", "Europe/Paris"],
]

stopsData = [
    ["stop_id", "stop_name", "stop_lat", "stop_lon"],
    ["1", "Paris Gare de Lyon", "48.8434", "2.3734"],
    ["2", "Lyon Part-Dieu", "45.7645", "4.8357"],
    ["3", "Marseille Saint-Charles", "43.3029", "5.3665"],
]

routesData = [
    ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
    ["1", "1", "TGV1", "Paris - Marseille", "2"],
]

calendarData = [
    ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
    ["1", "1", "1", "1", "1", "1", "0", "0", "20250101", "20251231"],
]

tripsData = [
    ["route_id", "service_id", "trip_id", "trip_headsign"],
    ["1", "1", "TGV1_001", "Marseille"],
    ["1", "1", "TGV1_002", "Marseille"],
]

stopTimesData = [
    ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
    ["TGV1_001", "07:00:00", "07:05:00", "1", "1"],
    ["TGV1_001", "10:30:00", "10:35:00", "2", "2"],
    ["TGV1_001", "13:00:00", "13:00:00", "3", "3"],
    ["TGV1_002", "14:00:00", "14:05:00", "1", "1"],
    ["TGV1_002", "17:30:00", "17:35:00", "2", "2"],
    ["TGV1_002", "20:00:00", "20:00:00", "3", "3"],
]

with zipfile.ZipFile(zipPath, "w") as zipBuffer:
    zipBuffer.writestr("agency.txt", _writeCsv(agencyData))
    zipBuffer.writestr("stops.txt", _writeCsv(stopsData))
    zipBuffer.writestr("routes.txt", _writeCsv(routesData))
    zipBuffer.writestr("calendar.txt", _writeCsv(calendarData))
    zipBuffer.writestr("trips.txt", _writeCsv(tripsData))
    zipBuffer.writestr("stop_times.txt", _writeCsv(stopTimesData))

print(f"GTFS créé : {zipPath}")
print(f"Taille : {zipPath.stat().st_size} bytes")
