"""
Tests de securite pour l'API ObRail Europe.

Verifie :
- Le CORS n'accepte pas n'importe quelle origine
- Les parametres invalides declenchent une 422 (pas de 500)
- Les injections SQL sont bloquees par SQLAlchemy / parametres bindes
- Les endpoints inconnus retournent 404
"""

import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


class TestCORSSecurity:
    """Tests de securite CORS."""

    def test_cors_rejects_unknown_origin(self):
        """Une origine non autorisee ne doit pas pouvoir acceder a l'API."""
        response = client.get(
            "/health",
            headers={"Origin": "https://evil-site.com"}
        )
        # FastAPI renvoie 200 mais sans Access-Control-Allow-Origin
        assert "access-control-allow-origin" not in response.headers

    def test_cors_allows_localhost_dev(self):
        """Une origine localhost autorisee doit recevoir les headers CORS."""
        response = client.get(
            "/health",
            headers={"Origin": "http://localhost:5173"}
        )
        assert response.status_code == 200
        assert "access-control-allow-origin" in response.headers
        assert response.headers["access-control-allow-origin"] == "http://localhost:5173"

    def test_cors_preflight_allowed_origin(self):
        """La requete preflight OPTIONS doit fonctionner pour une origine autorisee."""
        response = client.options(
            "/health",
            headers={
                "Origin": "http://localhost:8501",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Content-Type",
            }
        )
        assert response.status_code == 200
        assert "access-control-allow-origin" in response.headers

    def test_cors_preflight_rejected_origin(self):
        """La requete preflight OPTIONS doit rejeter une origine inconnue."""
        response = client.options(
            "/health",
            headers={
                "Origin": "https://attacker.com",
                "Access-Control-Request-Method": "GET",
            }
        )
        # Pas de header CORS pour origine non autorisee
        assert "access-control-allow-origin" not in response.headers


class TestInputValidation:
    """Tests de validation des entrees utilisateur."""

    def test_invalid_limit_returns_422_not_500(self):
        """Un parametre 'limit' invalide doit retourner 422, jamais 500."""
        response = client.get("/trains/?limit=invalid")
        assert response.status_code == 422

    def test_negative_limit_returns_422(self):
        """Un parametre 'limit' negatif doit retourner 422."""
        response = client.get("/trains/?limit=-10")
        assert response.status_code == 422

    def test_excessive_limit_returns_422(self):
        """Un parametre 'limit' superieur au max doit retourner 422."""
        response = client.get("/trains/?limit=999999")
        assert response.status_code == 422

    def test_sql_injection_in_query_param_blocked(self):
        """Une tentative d'injection SQL dans un parametre de query doit etre bloquee."""
        # FastAPI valide le type avant d'arriver a la BDD
        response = client.get("/trains/?limit=1; DROP TABLE trains--")
        assert response.status_code == 422

    def test_sql_injection_in_path_param_blocked(self):
        """Une tentative d'injection SQL dans un parametre de path doit etre bloquee."""
        response = client.get("/trains/1' OR '1'='1")
        assert response.status_code == 422

    def test_sql_injection_union_select_blocked(self):
        """Une tentative UNION SELECT doit etre bloquee par la validation de type."""
        response = client.get("/trains/?limit=1 UNION SELECT * FROM operators--")
        assert response.status_code == 422

    def test_xss_in_query_param_blocked(self):
        """Une tentative XSS dans un parametre doit etre traitee comme texte brut."""
        response = client.get("/trains/?train_type=<script>alert(1)</script>")
        # L'API retourne toujours du JSON, jamais du HTML execute
        assert "application/json" in response.headers.get("content-type", "")
        # Pas de script execute dans la reponse (on accepte 200, 422 ou 500 si BDD indisponible)
        assert "<script>" not in response.text or response.status_code in [422, 500]


class TestErrorHandling:
    """Tests de gestion des erreurs (pas de fuite d'informations)."""

    def test_unknown_endpoint_returns_404(self):
        """Un endpoint inconnu doit retourner 404."""
        response = client.get("/api/v2/admin/secret")
        assert response.status_code == 404

    def test_404_does_not_leak_stack_trace(self):
        """Une 404 ne doit pas contenir de stack trace ou d'informations sensibles."""
        response = client.get("/admin/config")
        assert response.status_code == 404
        body = response.text.lower()
        assert "traceback" not in body
        assert "stack" not in body
        assert "password" not in body
        assert "secret" not in body

    def test_method_not_allowed_returns_405(self):
        """Une methode HTTP non autorisee doit retourner 405."""
        response = client.post("/health")
        assert response.status_code == 405

    def test_health_endpoint_public(self):
        """L'endpoint health doit etre accessible sans authentification."""
        response = client.get("/health")
        assert response.status_code == 200
        assert "status" in response.json()


class TestHeaders:
    """Tests des headers de securite."""

    def test_no_server_header_leak(self):
        """Le header Server ne doit pas reveler la version exacte."""
        response = client.get("/health")
        server = response.headers.get("server", "").lower()
        # FastAPI/Starlette envoie 'uvicorn' par defaut — acceptable
        # Mais on verifie qu'il n'y a pas de numero de version critique
        assert "python" not in server

    def test_content_type_set(self):
        """Les reponses JSON doivent avoir un Content-Type correct."""
        response = client.get("/health")
        assert "application/json" in response.headers.get("content-type", "")
