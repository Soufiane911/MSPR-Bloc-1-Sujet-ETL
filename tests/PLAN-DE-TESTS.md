# Plan de Tests — ObRail Europe

## Objectif

Ce document decrit la strategie de tests du projet ObRail Europe. L'objectif est de garantir la fiabilite, la securite et la conformite de l'application avant chaque mise en production.

## Types de tests

| Type | Couverture | Outil | Localisation |
|------|-----------|-------|-------------|
| Tests unitaires | Composants backend (services, routers) | pytest | `tests/`, `api/tests/`, `etl/tests/` |
| Tests d'integration | API + base de donnees | pytest + TestClient | `tests/test_api_endpoints.py` |
| Tests de validation | Donnees ETL (qualite, format) | pytest | `tests/test_data_validation.py` |
| Tests de securite | CORS, injection, gestion erreurs | pytest | `tests/test_security.py` |
| Tests E2E | Parcours utilisateur frontend | Playwright | `frontend/e2e/` |
| Tests de performance | Latence, charge API | Apache Bench (ab) | CI/CD |
| Tests de monitoring | Stack Prometheus/Grafana | curl + promtool | CI/CD |

## Tests unitaires

### Backend API (`tests/test_api_endpoints.py`)

- **Trains** : lister, filtrer, pagination, limite invalide (422), train par ID, train inexistant (404)
- **Stations** : lister, filtrer par pays, filtrer par ville
- **Schedules** : lister les dessertes
- **Operators** : lister les operateurs
- **Erreurs** : endpoint inconnu (404), parametre invalide (422)
- **Documentation** : Swagger (/docs), ReDoc (/redoc), schema OpenAPI (/openapi.json)

> Techniques : mocking des services avec `@patch` pour isoler les tests de la BDD.

### ETL (`etl/tests/`)

- **Extracteurs** : lecture GTFS, JSON, CSV ; gestion des fichiers manquants
- **Transformers** : nettoyage, normalisation, classification jour/nuit
- **Loaders** : chargement idempotent en base
- **Freshness** : detection de changement de source

### Securite (`tests/test_security.py`)

- **CORS** : origines autorisees vs rejetees, preflight OPTIONS
- **Validation** : parametres invalides retournent 422 (pas 500)
- **Injection SQL** : tentatives bloquees par la validation de type FastAPI
- **XSS** : parametres traites comme texte brut
- **Gestion erreurs** : pas de fuite de stack trace ni de secrets

## Tests d'integration

### API + PostgreSQL

- L'API repond correctement lorsque la base est disponible
- L'endpoint `/health` retourne `connected` quand PostgreSQL est up
- Les requetes retournent des donnees coherences avec le schema

> Ces tests necessitent un service PostgreSQL actif. En CI, ils sont executes via le service Docker `postgres` dans `python-tests.yml`.

## Tests End-to-End (E2E)

### Frontend (`frontend/e2e/`)

- **smoke.spec.js** : le dashboard charge, les KPI s'affichent
- **navigation.spec.js** : navigation entre les onglets
- **filters.spec.js** : filtrage par pays, type de train, distance
- **business-dashboard.spec.js** : verification des indicateurs metier
- **accessibility.spec.js** : langue FR, skip link, focus visible, contraste, alt images

> Execution : `npm run e2e` (Playwright + Chromium)

## Tests de performance

- **Charge API** : 100 requetes concurrentes (10 clients) via `ab`
- **Surcharge** : verification du taux d'erreur < 5%
- **Metrics** : verification que `http_requests_total` est expose

## Tests de monitoring

- **Prometheus** : configuration valide (`promtool check config`)
- **Grafana** : dashboards JSON valides
- **Health checks** : API, Prometheus, Grafana, PostgreSQL exporter, cAdvisor

## Execution en CI/CD

Le workflow `.github/workflows/python-tests.yml` execute automatiquement :

1. Tests pytest (unitaires + securite)
2. Rapport de couverture (Codecov)
3. Demarrage Docker Compose complet
4. Health checks sur tous les services
5. Tests E2E Playwright
6. Tests de performance (Apache Bench)
7. Validation de la stack monitoring

## Matrice de couverture

| Composant | Unitaires | Integration | E2E | Securite | Perf |
|-----------|-----------|-------------|-----|----------|------|
| API | ✅ | ✅ | — | ✅ | ✅ |
| ETL | ✅ | — | — | — | — |
| Frontend | — | — | ✅ | — | — |
| Docker | — | ✅ | — | — | — |
| Monitoring | — | ✅ | — | — | — |

## Instructions d'execution locale

```bash
# Tous les tests pytest
pytest -q

# Uniquement les tests de securite
pytest tests/test_security.py -v

# Tests E2E
npm --prefix frontend run e2e

# Avec Docker Compose
docker compose up -d
pytest tests/
npm --prefix frontend run e2e
```

## Maintenance

- Ajouter un test pour chaque nouveau endpoint ou fonctionnalite critique
- Maintenir les tests E2E a jour lors des changements d'UI
- Verifier la couverture de code a chaque sprint (> 70% recommande)
