def testTrajetsFiltersByNightAndAgency(client):
    response = client.post("/import/seed")
    assert response.status_code == 200

    allResponse = client.get("/trajets?limit=50&offset=0")
    assert allResponse.status_code == 200
    allData = allResponse.get_json()
    assert allData["total"] >= 1

    sample = allData["trajets"][0]
    agencyId = sample["agence"]["id"]

    dayResponse = client.get("/trajets?is_night=false&limit=50&offset=0")
    assert dayResponse.status_code == 200
    day = dayResponse.get_json()
    for trajet in day["trajets"]:
        assert trajet["trainDeNuit"] is False

    nightResponse = client.get("/trajets?is_night=true&limit=50&offset=0")
    assert nightResponse.status_code == 200
    night = nightResponse.get_json()
    for trajet in night["trajets"]:
        assert trajet["trainDeNuit"] is True

    agencyResponse = client.get(f"/trajets?agency_id={agencyId}&limit=50&offset=0")
    assert agencyResponse.status_code == 200
    agencyData = agencyResponse.get_json()
    for trajet in agencyData["trajets"]:
        assert trajet["agence"]["id"] == agencyId
