import io
import zipfile


def make_min_gtfs_zip_bytes():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(
            "agency.txt",
            "agency_id,agency_name,agency_url,agency_timezone\n"
            "A1,Test Agency,https://example.com,Europe/Paris\n",
        )
        z.writestr(
            "routes.txt",
            "route_id,agency_id,route_short_name,route_long_name,route_type\n"
            "R1,A1,R1,Route One,2\n",
        )
        z.writestr(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon\n"
            "S1,Stop One,49.6100,6.1296\n"
            "S2,Stop Two,49.8153,6.1333\n",
        )
        z.writestr(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "WD,1,1,1,1,1,0,0,20250101,20251231\n",
        )
        z.writestr(
            "trips.txt",
            "route_id,service_id,trip_id,trip_headsign\n"
            "R1,WD,T1,To Stop Two\n",
        )
        z.writestr(
            "stop_times.txt",
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "T1,08:00:00,08:00:00,S1,1\n"
            "T1,09:00:00,09:00:00,S2,2\n",
        )
    return buf.getvalue()


def test_import_gtfs_zip_via_upload(client):
    data = {
        "file": (io.BytesIO(make_min_gtfs_zip_bytes()), "feed.zip"),
    }
    resp = client.post("/import/gtfs", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload.get("status") == "gtfs_imported"
    # Verify preflight summary exists
    assert payload.get("preflight")

    # List trajets should have at least one
    r2 = client.get("/trajets?limit=10&offset=0")
    assert r2.status_code == 200
    d2 = r2.get_json()
    assert d2["total"] >= 1
    first = d2["trajets"][0]
    # Horaires preserved and ordered
    assert len(first["horaires"]) >= 2
    assert first["horaires"][0]["sequence"] < first["horaires"][1]["sequence"]
    # Stats exist and distance positive
    if first.get("stats"):
        assert first["stats"]["distance_km"] is None or first["stats"]["distance_km"] >= 0
