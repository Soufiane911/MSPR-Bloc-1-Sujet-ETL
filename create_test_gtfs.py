#!/usr/bin/env python3
"""Create a minimal GTFS ZIP file for testing."""

import csv
import zipfile
from io import StringIO
from pathlib import Path

def _write_csv(data):
    """Helper to write CSV data."""
    output = StringIO()
    writer = csv.writer(output)
    for row in data:
        writer.writerow(row)
    return output.getvalue()

gtfs_dir = Path(__file__).parent
zip_path = gtfs_dir / "test_gtfs.zip"

# Create CSV data
agency_data = [
    ["agency_id", "agency_name", "agency_url", "agency_timezone"],
    ["1", "SNCF Test", "https://sncf.com", "Europe/Paris"],
]

stops_data = [
    ["stop_id", "stop_name", "stop_lat", "stop_lon"],
    ["1", "Paris Gare de Lyon", "48.8434", "2.3734"],
    ["2", "Lyon Part-Dieu", "45.7645", "4.8357"],
    ["3", "Marseille Saint-Charles", "43.3029", "5.3665"],
]

routes_data = [
    ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type"],
    ["1", "1", "TGV1", "Paris - Marseille", "2"],  # 2 = Train
]

calendar_data = [
    ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
    ["1", "1", "1", "1", "1", "1", "0", "0", "20250101", "20251231"],
]

trips_data = [
    ["route_id", "service_id", "trip_id", "trip_headsign"],
    ["1", "1", "TGV1_001", "Marseille"],
    ["1", "1", "TGV1_002", "Marseille"],
]

stop_times_data = [
    ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
    ["TGV1_001", "07:00:00", "07:05:00", "1", "1"],
    ["TGV1_001", "10:30:00", "10:35:00", "2", "2"],
    ["TGV1_001", "13:00:00", "13:00:00", "3", "3"],
    ["TGV1_002", "14:00:00", "14:05:00", "1", "1"],
    ["TGV1_002", "17:30:00", "17:35:00", "2", "2"],
    ["TGV1_002", "20:00:00", "20:00:00", "3", "3"],
]

# Write ZIP
with zipfile.ZipFile(zip_path, 'w') as z:
    z.writestr("agency.txt", _write_csv(agency_data))
    z.writestr("stops.txt", _write_csv(stops_data))
    z.writestr("routes.txt", _write_csv(routes_data))
    z.writestr("calendar.txt", _write_csv(calendar_data))
    z.writestr("trips.txt", _write_csv(trips_data))
    z.writestr("stop_times.txt", _write_csv(stop_times_data))

print(f"✅ GTFS créé : {zip_path}")
print(f"📦 Taille : {zip_path.stat().st_size} bytes")
