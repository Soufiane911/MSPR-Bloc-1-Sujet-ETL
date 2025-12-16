import io
import zipfile


def makeMinGtfsZipBytes():
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zipBuffer:
        zipBuffer.writestr(
            "agency.txt",
            "agency_id,agency_name,agency_url,agency_timezone\n" "A1,Test Agency,https://example.com,Europe/Paris\n",
        )
        zipBuffer.writestr(
            "routes.txt",
            "route_id,agency_id,route_short_name,route_long_name,route_type\n" "R1,A1,R1,Route One,2\n",
        )
        zipBuffer.writestr(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon\n" "S1,Stop One,49.6100,6.1296\n" "S2,Stop Two,49.8153,6.1333\n",
        )
        zipBuffer.writestr(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n" "WD,1,1,1,1,1,0,0,20250101,20251231\n",
        )
        zipBuffer.writestr(
            "trips.txt",
            "route_id,service_id,trip_id,trip_headsign\n" "R1,WD,T1,To Stop Two\n",
        )
        zipBuffer.writestr(
            "stop_times.txt",
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n" "T1,08:00:00,08:00:00,S1,1\n" "T1,09:00:00,09:00:00,S2,2\n",
        )
    return buffer.getvalue()


def testImportGtfsZipViaUpload(client):
    data = {"file": (io.BytesIO(makeMinGtfsZipBytes()), "feed.zip")}
    resp = client.post("/import/gtfs", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload.get("status") == "gtfsImported"
    assert payload.get("preflight")

    responseTrajets = client.get("/trajets?limit=10&offset=0")
    assert responseTrajets.status_code == 200
    trajetsData = responseTrajets.get_json()
    assert trajetsData["total"] >= 1
    first = trajetsData["trajets"][0]
    assert len(first["horaires"]) >= 2
    assert first["horaires"][0]["sequence"] < first["horaires"][1]["sequence"]
    if first.get("stats"):
        assert first["stats"]["distanceKm"] is None or first["stats"]["distanceKm"] >= 0
