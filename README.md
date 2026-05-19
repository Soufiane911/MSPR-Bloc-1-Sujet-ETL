# ObRail Europe

Projet MSPR TPRE612 consacre a la collecte, la transformation et l'analyse de donnees ferroviaires europeennes afin d'etudier la contribution respective des trains de jour et des trains de nuit au maillage ferroviaire.

## Presentation du projet

ObRail Europe met en oeuvre une chaine ETL complete fondee sur des sources ouvertes et semi-ouvertes du domaine ferroviaire europeen. Le projet poursuit trois objectifs principaux :

- constituer un entrepot de donnees ferroviaires structurees a partir de sources heterogenes ;
- classifier les services ferroviaires selon une logique metier jour/nuit ;
- exposer les resultats par une API REST et un tableau de bord analytique.

L'approche retenue privilegie les flux grande ligne et longue distance, plus pertinents pour une comparaison entre offres diurnes et nocturnes, plutot qu'une collecte exhaustive de l'ensemble des transports ferroviaires locaux ou regionaux.

## Problematique

La question centrale du projet est la suivante : dans quelle mesure les trains de jour et les trains de nuit contribuent-ils au maillage ferroviaire europeen ?

Cette problematique suppose :

- une integration de sources multi-pays ;
- une harmonisation de formats heterogenes ;
- une distinction robuste entre services diurnes et nocturnes ;
- des indicateurs permettant d'analyser la couverture, la connectivite et les dessertes.

## Architecture generale

Le systeme est compose de quatre couches principales :

1. une couche de sources de donnees externes ;
2. un pipeline ETL developpe en Python ;
3. une base PostgreSQL structurante ;
4. une couche d'exposition composee d'une API FastAPI et d'un dashboard React (avec un dashboard Dash optionnel).

```text
Sources de donnees
    -> ETL Python (extraction, nettoyage, normalisation, classification, chargement)
    -> PostgreSQL
    -> API REST FastAPI
    -> Dashboard React (Nginx) + Dash (optionnel)
```

## Perimetre des sources

Le perimetre retenu pour la version finale de l'ETL repose sur des sources directement utiles a l'etude des services longue distance et des circulations nocturnes.

### Sources principales

| Source | Role principal | Pays / zone | Format |
|---|---|---|---|
| Back-on-Track Night Train Database | Reference metier pour les trains de nuit | Europe | JSON |
| Deutsche Bahn - Fernverkehr (`fv_free`) | Grandes lignes allemandes | Allemagne | GTFS |
| SNCF Intercites | Services inter-regionaux et Intercites de Nuit | France | GTFS |
| Renfe AVE / longue distance | Grandes lignes espagnoles | Espagne | GTFS |
| Trenitalia | Services longue distance italiens | Italie | GTFS |
| CFF/SBB | Axes transfrontaliers et corridors centraux | Suisse | GTFS |
| SNCB | Hub belge et connexions nord-europeennes | Belgique | GTFS |

### Source de support

| Source | Role | Format |
|---|---|---|
| Mobility Database Catalogs | Catalogue de metadonnees pour la maintenance et l'extension future du projet | CSV |

### Sources exclues de la comparaison principale

- `SNCF Transilien`, car le flux est majoritairement urbain et periurbain ;
- `GTFS Allemagne rv_free`, car il introduit un bruit regional important par rapport a `fv_free` ;
- `OEBB`, tant qu'une URL GTFS stable et verifiee n'est pas disponible.

## Structure du depot

```text
obrail-mspr/
|-- api/
|   `-- app/
|       |-- models/
|       |-- routers/
|       `-- services/
|-- dashboard/
|   `-- app/
|       `-- pages/
|-- data/
|   |-- raw/
|   `-- processed/
|-- etl/
|   |-- analysis/
|   |-- config/
|   |-- extractors/
|   |-- loaders/
|   |-- transformers/
|   `-- main.py
|-- sql/
|   |-- init/
|   `-- schema.sql
|-- docker-compose.yml
`-- README.md
```

## Composants applicatifs

### ETL

Le pipeline ETL assure les fonctions suivantes :

- verification et telechargement conditionnel des sources ;
- extraction des donnees GTFS, JSON et CSV ;
- nettoyage et normalisation des champs ;
- classification metier jour/nuit ;
- chargement idempotent dans PostgreSQL.

Le point d'entree principal est `etl/main.py`.

### Base de donnees

La base PostgreSQL repose principalement sur quatre tables metier :

- `operators`
- `stations`
- `trains`
- `schedules`

Le schema inclut egalement des vues analytiques destinees au dashboard et a l'API.

### API

L'API REST expose les donnees agregees et detaillees via FastAPI. Elle permet notamment de consulter :

- les trains et leurs caracteristiques ;
- les dessertes et horaires ;
- les operateurs et gares ;
- les statistiques de synthese.

### Dashboard

Le dashboard React fournit une interface de consultation des indicateurs de qualite, des comparaisons jour/nuit et des vues synthetiques par pays, operateur ou type de service. Un dashboard Dash alternatif est egalement disponible sur le port 8050.

## Methodologie de classification jour/nuit

La classification repose sur des regles metier combinees, et non sur une simple heure de depart. La logique generale suit les principes suivants :

1. la presence d'indices explicites de couchage ou de service de nuit constitue une preuve forte en faveur de la classe `night` ;
2. la duree du trajet et la proportion de circulation en periode nocturne sont prises en compte pour les cas ambigus ;
3. les autres services sont classes dans la categorie `day`.

Les periodes temporelles utilisees dans le projet sont :

- jour : de 05:00 a 20:59 ;
- nuit : de 21:00 a 04:59.

## Installation et execution

### Prerequis

- Docker et Docker Compose ;
- Python 3.11 ou version superieure pour une execution locale ;
- un environnement compatible PostgreSQL si le projet est lance hors conteneurs.

### Lancement rapide (recommande)

Deux scripts sont fournis pour simplifier le lancement du projet. Ils vérifient automatiquement que Docker Desktop est installé et démarré, puis lancent l'ETL.

#### macOS

Double-cliquez sur le fichier `launch-etl.command` ou exécutez-le dans un terminal :

```bash
./launch-etl.command
```

#### Windows

Double-cliquez sur le fichier `launch-etl.bat` ou exécutez-le dans l'invite de commandes :

```cmd
launch-etl.bat
```

**Fonctionnement des scripts :**
1. Vérifient si Docker Desktop est installé (message d'erreur sinon)
2. Démarrent Docker Desktop s'il ne l'est pas déjà
3. Attendent que Docker soit prêt
4. Lancent PostgreSQL
5. Attendent 30 secondes (initialisation de la base)
6. Exécutent le pipeline ETL
7. Affichent les URLs d'accès aux services

### Lancement manuel avec Docker

Si vous préférez ne pas utiliser les scripts, vous pouvez lancer manuellement :

```bash
docker-compose up -d
sleep 30
docker-compose --profile etl run --rm etl python main.py --force
```

### Deploiement staging

Pour deployer l'application en environnement de staging (pre-production), un fichier dedie et un script d'automatisation sont fournis :

- `docker-compose.staging.yml` : orchestration des services avec images GHCR ou builds locaux
- `scripts/deploy-staging.sh` : script de deploiement avec health checks automatiques

**Prerequis staging :**
- Docker et Docker Compose v2
- Un fichier `.env.staging` (cree automatiquement depuis `.env.example` si absent)
- (Optionnel) `GHCR_TOKEN` pour tirer les images pre-construites depuis GitHub Container Registry

**Lancement rapide :**

```bash
# Avec images GHCR (recommande si le workflow CD est vert)
./scripts/deploy-staging.sh --pull

# Avec build local des images
./scripts/deploy-staging.sh --build

# Sans argument : demarre avec les images disponibles localement
./scripts/deploy-staging.sh
```

**Ce que fait le script :**
1. Verifie que Docker est installe et demarre
2. Cree `.env.staging` a partir de `.env.example` si necessaire
3. Connecte a GHCR (si `GHCR_TOKEN` est defini)
4. Arrete proprement l'ancienne stack staging
5. Lance la nouvelle stack avec `docker-compose.staging.yml`
6. Execute des health checks sur l'API, Grafana et Prometheus
7. Affiche un recapitulatif des URLs accessibles

**Arret du staging :**

```bash
docker compose -f docker-compose.staging.yml down
```

### Services exposes

| Service | Adresse | Description |
|---|---|---|
| API REST | `http://localhost:8000` | Service FastAPI |
| Documentation OpenAPI | `http://localhost:8000/docs` | Interface Swagger |
| Dashboard | `http://localhost:8501` | Frontend React (Nginx) |
| PostgreSQL | `localhost:5433` | Base relationnelle |

## Commandes utiles

### ETL

```bash
cd etl
python main.py --status
python main.py --force
python main.py --source back_on_track
```

### API

```bash
curl "http://localhost:8000/stats/summary"
curl "http://localhost:8000/trains?train_type=night&limit=10"
curl "http://localhost:8000/stats/day-night"
```

### Analyse complementaire

```bash
cd etl
python analysis/ml_classifier.py
```

## Environnement de developpement

### Prerequis

- **Python 3.11** (version recommandee et utilisee en CI/CD)
- **Docker** et **Docker Compose v2**
- **Node.js 20** (pour le frontend)

### Installation locale (venv Python 3.11)

Pour eviter les conflits de dependances, il est recommande d'utiliser un environnement virtuel Python 3.11 :

```bash
# Creer le venv
python3.11 -m venv .venv

# Activer (macOS / Linux)
source .venv/bin/activate

# Activer (Windows)
# .venv\Scripts\activate

# Installer les dependances
pip install -r api/requirements.txt -r etl/requirements.txt -r requirements-test.txt

# Verifier l'installation
pip check
```

> **Note** : L'environnement local doit utiliser Python 3.11 et les versions pincees des dependances. L'utilisation de Python 3.13 ou de versions flottantes peut provoquer des incompatibilites (ex. FastAPI / Starlette).

### Lancer les tests

#### Tests unitaires et securite (rapide, sans base de donnees)

```bash
# Tous les tests (depuis la racine du repo)
pytest -q

# Avec couverture de code
pytest --cov=. --cov-report=xml --cov-report=term

# Tests de securite uniquement
pytest tests/test_security.py -v
```

#### Tests d'integration (requiert PostgreSQL)

Les tests d'integration verifient l'API REST contre une vraie base PostgreSQL.
Ils sont ignores si la base n'est pas accessible.

```bash
# 1. Demarrer PostgreSQL (obligatoire)
docker compose up -d database

# 2. Attendre l'initialisation (healthcheck automatique)
docker compose exec database pg_isready -U obrail -d obrail_db

# 3. Executer les tests d'integration
pytest tests/test_integration.py -v

# Alternative : tout en une commande
docker compose up -d database && \
  echo "Attente initialisation..." && \
  sleep 10 && \
  pytest tests/test_integration.py -v
```

> **Note** : Sans PostgreSQL, les tests d'integration sont automatiquement ignores (`pytest.skip`).
> La CI/CD demarre un service PostgreSQL dedie pour ces tests.

#### Tests frontend E2E (Playwright)

```bash
cd frontend
npm ci
npx playwright install --with-deps chromium
npm run e2e
```

### Execution hors Docker (mode local)

```bash
# 1. Demarrer PostgreSQL (port 5433)
docker compose up -d database

# 2. Attendre l'initialisation de la base (30s)
sleep 30

# 3. Charger les donnees ETL
cd etl
python main.py --force

# 4. Lancer l'API
cd ../api
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# 5. Lancer le dashboard (dans un autre terminal)
cd ../frontend
npm install
npm run dev
```

---

## Validation et tests

Le depot contient une suite de tests complete couvrant plusieurs niveaux :

| Type | Fichier | Description |
|------|---------|-------------|
| Unitaires API | `tests/test_api_endpoints.py` | Endpoints REST (mockes) |
| API main | `api/tests/test_main.py` | Tests API avec SQLite |
| Securite | `tests/test_security.py` | CORS, injection, validation |
| Validation donnees | `tests/test_data_validation.py` | Qualite des donnees |
| Merger | `tests/test_data_merger.py` | Fusion des donnees |
| ETL pipeline | `tests/test_etl_pipeline.py` | Pipeline complet |
| ETL loader | `etl/tests/test_database_loader.py` | Chargement BDD |
| ETL extracteurs | `etl/tests/test_extractors_local_files.py` | Extraction sources |
| ETL fraicheur | `etl/tests/test_freshness.py` | Detection changements |
| ETL classifier | `etl/tests/test_day_night_classifier.py` | Classification jour/nuit |
| Integration | `tests/test_integration.py` | API + PostgreSQL reelle |
| E2E Frontend | `frontend/e2e/` | Playwright (smoke, accessibilite) |

```bash
pytest -q
```

Il est egalement possible d'executer les sous-ensembles de tests depuis les repertoires concernes selon les besoins de validation locale.

## Limites connues

- certaines sources GTFS peuvent evoluer ou devenir indisponibles sans preavis ;
- la qualification jour/nuit depend de la qualite et de la granularite des donnees sources ;
- certaines sources nationales necessitent un filtrage metier pour exclure les dessertes trop locales au regard de la problematique ;
- l'URL actuellement utilisee pour la Suisse doit etre verifiee regulierement afin de confirmer qu'elle reste directement exploitable dans le pipeline.

## Donnees et conformite RGPD

Le projet ObRail Europe traite exclusivement des **donnees publiques et ouvertes** du domaine ferroviaire. Aucune donnee a caractere personnel (DCP) n'est collectee, stockee ou traitee.

### Sources et licences

Les jeux de donnees proviennent de sources open data europeennes (Back-on-Track, Deutsche Bahn, SNCF, Renfe, Trenitalia, SNCB, Mobility Database Catalogs). Les licences respectives (GPL-3.0, ODbL, CC-BY-4.0) imposent l'attribution et, pour l'ODbL, le partage a l'identique.

### Logs et retention

- **Contenu des logs** : evenements techniques uniquement (timestamp, endpoint, statut HTTP, duree). Aucune donnee personnelle ni token d'authentification n'est loggue.
- **Duree de conservation** :
  - Logs conteneurises (Loki) : 7 jours
  - Logs ETL (fichier local) : 7 jours
  - Logs systeme : 7 jours
- Les logs ETL sont stockes dans `logs/` (ignore par `.gitignore`). Les logs conteneurises sont agreges via Loki en environnement Docker.

### Mesures de securite complementaires

- Les fichiers `.env` et `.env.staging` sont exclus du versionnement
- Les secrets CI/CD utilisent les mecanismes natifs GitHub Actions (`secrets.GITHUB_TOKEN`)
- La base PostgreSQL n'est exposee que sur localhost (port 5433)

Pour le detail complet, voir `docs/RGPD.md`.

## Documentation technique

| Document | Description |
|----------|-------------|
| `docs/RGPD.md` | Conformite RGPD et gestion des donnees |
| `docs/RGAA-AUDIT.md` | Audit d'accessibilite RGAA/WCAG du dashboard |
| `docs/SECURITY-HTTPS-RATELIMIT.md` | Recommandations HTTPS et rate limiting pour la production |
| `docs/ALERTING.md` | Guide des alertes Prometheus (runbook) |
| `PLAN-DE-TESTS.md` | Strategie et couverture des tests |

## Licences des donnees

Les donnees integrees dans le projet demeurent soumises aux conditions de licence propres a chaque fournisseur. A titre indicatif :

- Back-on-Track : GPL-3.0 ;
- SNCF : ODbL ;
- Deutsche Bahn : CC-BY-4.0 ;
- Renfe : CC-BY-4.0 ;
- Trenitalia : licence a verifier selon la source ;
- CFF/SBB et SNCB : selon les conditions de diffusion des producteurs concernes.

## Cadre academique

Ce projet est realise dans le cadre du MSPR TPRE612 de la certification professionnelle Developpeur IA et Data Science.
