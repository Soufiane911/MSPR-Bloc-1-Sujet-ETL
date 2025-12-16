def testHealthOk(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data.get("status") == "healthy"
    assert data.get("database") == "ok"


def testPreflightMissingUrlReturns400(client):
    resp = client.get("/import/gtfs/preflight")
    assert resp.status_code == 400
    data = resp.get_json()
    assert data.get("error") == "missingUrl"
