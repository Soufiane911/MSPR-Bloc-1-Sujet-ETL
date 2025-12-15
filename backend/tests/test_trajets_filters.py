def test_trajets_filters_by_night_and_agency(client):
    # Seed minimal data
    r = client.post("/import/seed")
    assert r.status_code == 200

    # Fetch all
    all_r = client.get("/trajets?limit=50&offset=0")
    assert all_r.status_code == 200
    all_data = all_r.get_json()
    assert all_data["total"] >= 1

    # Grab one agency id from results
    sample = all_data["trajets"][0]
    agency_id = sample["agence"]["id"]

    # Filter by day
    day_r = client.get("/trajets?is_night=false&limit=50&offset=0")
    assert day_r.status_code == 200
    day = day_r.get_json()
    for t in day["trajets"]:
        assert t["train_de_nuit"] is False

    # Filter by night
    night_r = client.get("/trajets?is_night=true&limit=50&offset=0")
    assert night_r.status_code == 200
    night = night_r.get_json()
    for t in night["trajets"]:
        assert t["train_de_nuit"] is True

    # Filter by agency
    ag_r = client.get(f"/trajets?agency_id={agency_id}&limit=50&offset=0")
    assert ag_r.status_code == 200
    ag = ag_r.get_json()
    for t in ag["trajets"]:
        assert t["agence"]["id"] == agency_id
