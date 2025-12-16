import os

from app.db import db


def testInitdbForbiddenWhenNotAllowed(monkeypatch):
    monkeypatch.setenv("FLASK_ENV", "production")
    monkeypatch.delenv("ALLOW_INITDB", raising=False)
    monkeypatch.setenv("DATABASE_URL", "sqlite:///forbidden.db")

    from app import createApp

    app = createApp()
    client = app.test_client()

    resp = client.post("/initdb")
    assert resp.status_code == 403
    assert resp.get_json().get("error") == "forbidden"


def testSeedImportAndTrajetsList(client, app):
    resp = client.post("/import/seed")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload.get("status") == "seedDataImported"

    resp2 = client.get("/trajets?limit=5&offset=0")
    assert resp2.status_code == 200
    data = resp2.get_json()
    assert data["total"] >= 1
    assert isinstance(data["trajets"], list)
    assert len(data["trajets"]) >= 1

    resp3 = client.get("/trajets?limit=1&offset=0")
    assert resp3.status_code == 200
    d3 = resp3.get_json()
    assert d3["limit"] == 1
    assert d3["offset"] == 0
    assert len(d3["trajets"]) == 1
