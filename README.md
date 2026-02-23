# 🚂 ObRail Europe - MSPR TPRE612

Projet de Mise en Situation Professionnelle Reconstituée (MSPR) pour le bloc E6.1 
"Créer un modèle de données d'une solution I.A en utilisant des méthodes de Data science".

## 📋 Description

Ce projet implémente un **processus ETL complet** pour ObRail Europe, un observatoire 
indépendant spécialisé dans le ferroviaire et la mobilité durable.

L'objectif est de comparer la contribution des **trains de jour et des trains de nuit** 
au maillage ferroviaire européen.

## 🏗️ Architecture

```
obrail-mspr/
├── 📁 etl/              # Scripts ETL (Extract, Transform, Load)
│   ├── extractors/      # Extracteurs de données
│   ├── transformers/    # Transformateurs de données
│   ├── loaders/         # Loaders vers PostgreSQL
│   └── config/          # Configuration
│
├── 📁 api/              # API REST FastAPI
│   └── app/
│       ├── routers/     # Endpoints API
│       ├── models/      # Modèles Pydantic
│       └── services/    # Services métier
│
├── 📁 dashboard/        # Dashboard Streamlit
│   └── app/
│       └── pages/       # Pages du dashboard
│
├── 📁 sql/              # Scripts SQL
│   ├── schema.sql       # Schéma de la base
│   └── init/            # Scripts d'initialisation
│
└── 📁 data/             # Données (créé automatiquement)
    ├── raw/             # Données brutes
    └── processed/       # Données transformées
```

## 🚀 Démarrage rapide

### Prérequis

- Docker et Docker Compose
- Python 3.11+ (optionnel, pour développement local)

### Lancement complet

```bash
# 1. Cloner le projet
git clone <url-du-projet>
cd obrail-mspr

# 2. Lancer l'infrastructure
docker-compose up -d database

# 3. Attendre que la base soit prête (30s environ)
sleep 30

# 4. Exécuter l'ETL
docker-compose --profile etl run --rm etl

# 5. Lancer l'API et le dashboard
docker-compose up -d api dashboard
```

### Accès aux services

| Service | URL | Description |
|---------|-----|-------------|
| API | http://localhost:8000 | API REST FastAPI |
| Documentation API | http://localhost:8000/docs | Swagger UI |
| Dashboard | http://localhost:8501 | Streamlit Dashboard |
| Base de données | localhost:5432 | PostgreSQL |

## 📊 Sources de données

### Tier 1 - Indispensables

| Source | Type | Pays | Format |
|--------|------|------|--------|
| Back-on-Track Night Train Database | GitHub/API | Europe | JSON |
| Mobility Database Catalogs | GitHub | Monde | CSV |
| SNCF (transport.data.gouv) | API/GTFS | France | GTFS |
| GTFS.de | Direct | Allemagne | GTFS |
| ÖBB | Direct | Autriche | GTFS |

### Tier 2 - Complémentaires

| Source | Type | Pays | Format |
|--------|------|------|--------|
| Renfe | Direct | Espagne | GTFS |
| Trenitalia | Direct | Italie | GTFS |
| Transitland | API | Monde | JSON |

## 🔧 Utilisation

### Exécution de l'ETL

```bash
# Exécution complète
docker-compose --profile etl run --rm etl

# Exécution pour une source spécifique
docker-compose --profile etl run --rm etl python main.py --source back_on_track
```

## 🧪 Tests

### Setup des Tests

Des tests unitaires, d'intégration et de sécurité ont été implémentés pour assurer la qualité du code.

#### Installation des dépendances de test

```bash
pip install -r requirements-test.txt
```

### Exécution des tests

**Tous les tests:**
```bash
pytest tests/ -v
```

**Tests spécifiques:**
```bash
# Tests de validation des données
pytest tests/test_data_validation.py -v

# Tests des endpoints API
pytest tests/test_api_endpoints.py -v

# Tests du pipeline ETL
pytest tests/test_etl_pipeline.py -v

# Tests du fusionneur de données
pytest tests/test_data_merger.py -v

# Tests d'exemple
pytest tests/test_example.py -v
```

**Avec rapport de couverture:**
```bash
pytest tests/ --cov=. --cov-report=html
```

### Suites de tests disponibles

#### 1. **Tests de validation des données** (`test_data_validation.py`)
- Validation des numéros de train
- Validation des coordonnées géographiques
- Validation des formats d'heure
- Validation des codes pays
- Validation des structures de DataFrames

#### 2. **Tests des endpoints API** (`test_api_endpoints.py`)
- Endpoints trains (GET, filtres, pagination)
- Endpoints stations (GET, filtres)
- Endpoints horaires
- Endpoints opérateurs
- Gestion des erreurs API
- Documentation API (Swagger, ReDoc)

#### 3. **Tests du pipeline ETL** (`test_etl_pipeline.py`)
- Extraction de données (GTFS, Back-on-Track, Mobility Catalog)
- Transformation et nettoyage
- Normalisation
- Classification jour/nuit
- Fusion de données
- Chargement en base de données
- Tests d'intégration complets

#### 4. **Tests du fusionneur de données** (`test_data_merger.py`)
- Fusion d'opérateurs multisources
- Fusion de stations
- Fusion de trains
- Résolution de conflits
- Intégrité des données
- Performance sur grandes données

#### 5. **Tests d'exemple** (`test_example.py`)
- Nettoyage de données
- Gestion des valeurs manquantes
- Tests de sécurité (injection SQL, XSS)

### GitHub Actions Workflow

Le fichier `.github/workflows/python-tests.yml` configure une intégration continue automatique:

- Les tests s'exécutent à chaque push et pull request
- Rapport de couverture généré automatiquement
- Upload vers Codecov pour le suivi
- Notification des résultats

### Bonnes pratiques de test

1. **Écrivez des tests isolés** - Utilisez des mocks pour les dépendances externes
2. **Nommez clairement** - Les noms de tests doivent décrire ce qu'ils testent
3. **Testez les cas limites** - Empty, null, invalid inputs
4. **Maintenez la couverture** - Visez 80%+ de couverture de code
5. **Exécutez régulièrement** - Avant chaque commit local et en CI/CD

### API REST

#### Endpoints principaux

| Endpoint | Méthode | Description |
|----------|---------|-------------|
| `/trains` | GET | Liste des trains |
| `/trains/{id}` | GET | Détail d'un train |
| `/schedules` | GET | Liste des dessertes |
| `/schedules/search` | GET | Recherche de trajets |
| `/stations` | GET | Liste des gares |
| `/operators` | GET | Liste des opérateurs |
| `/stats/summary` | GET | Statistiques globales |

#### Exemples de requêtes

```bash
# Liste des trains
curl http://localhost:8000/trains

# Trains de nuit
curl "http://localhost:8000/trains?train_type=night"

# Recherche de trajets
curl "http://localhost:8000/schedules/search?from_city=Paris&to_city=Lyon"
```

### Dashboard

Le dashboard Streamlit offre :
- **Vue d'ensemble** : KPIs et statistiques globales
- **Jour vs Nuit** : Comparaison détaillée
- **Opérateurs** : Analyse par opérateur
- **Carte** : Visualisation géographique
- **Qualité des données** : Contrôle de complétude

## 🗄️ Modèle de données

### Tables principales

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  operators  │────<│   trains    │────<│  schedules  │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                           │
                     ┌─────────────┐
                     │  stations   │
                     └─────────────┘
```

### Schéma détaillé

- **operators** : Opérateurs ferroviaires
- **stations** : Gares et arrêts
- **trains** : Trains avec classification jour/nuit
- **schedules** : Dessertes avec horaires

## 📦 Livrables

1. Scripts ETL opérationnels
2. Modèle conceptuel et physique des données
3. Base de données PostgreSQL alimentée
4. API REST fonctionnelle
5. Documentation technique
6. Tableau de bord de contrôle
7. Support de soutenance

## 📄 Licences

Les données sont fournies sous les licences respectives des sources :
- Back-on-Track : GPL-3.0
- SNCF : ODbL
- GTFS.de, ÖBB, Renfe : CC-BY-4.0

## 👥 Équipe

Projet réalisé dans le cadre de la certification professionnelle 
**Développeur en Intelligence Artificielle et Data Science** (RNCP 36581).

---

<p align="center">
  🚂 <strong>ObRail Europe</strong> - Pour une mobilité durable
</p>

## Unit Testing Setup

### Unit Tests

To ensure the quality of the code, unit tests are implemented using pytest. Follow the instructions below to set up and run the tests.

### Prerequisites
- Python 3.x
- pytest installed

### Installation
To install pytest, run the following command:

```bash
pip install pytest
```

### Creating Tests
1. Create a new directory named `tests` in the root of your project.
2. Inside the `tests` directory, create test files prefixed with `test_` (e.g., `test_example.py`).
3. Write your test functions inside these files.

Example test function:

```python
import pytest

from app.main import some_function


def test_some_function():
    assert some_function() == expected_value
```

### Running Tests
To run the tests, execute:

```bash
pytest
```

### GitHub Actions Workflow

A GitHub Actions workflow has been created to automate testing. The workflow file is located at `.github/workflows/python-tests.yml`. This file will run tests on every push and pull request to the main branch.

Make sure to check the workflow file for any specific configurations you might need to adjust based on your project requirements.

### Security Tests

Security tests have also been included to check for vulnerabilities such as SQL injection and XSS. These tests ensure that the application handles malicious input appropriately.

Make sure to run these tests regularly to maintain code quality and security.
