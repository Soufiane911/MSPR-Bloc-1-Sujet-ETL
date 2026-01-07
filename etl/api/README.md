# 🌐 API REST ObRail Europe

API REST développée avec FastAPI pour interroger les données ferroviaires européennes.

## 🚀 Démarrage

### Installation des dépendances

```bash
cd etl
pip install fastapi uvicorn
```

### Lancer l'API

```bash
cd etl
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

L'API sera accessible sur :
- **API** : http://localhost:8000
- **Documentation Swagger** : http://localhost:8000/docs
- **Documentation ReDoc** : http://localhost:8000/redoc

## 📚 Endpoints Disponibles

### 🏢 GTFS - Opérateurs (Agencies)

- `GET /api/agencies` - Liste des opérateurs
- `GET /api/agencies/{agency_id}` - Détails d'un opérateur

### 🚂 GTFS - Lignes (Routes)

- `GET /api/routes` - Liste des lignes
- `GET /api/routes/{route_id}` - Détails d'une ligne
- `GET /api/routes/stats` - Statistiques des lignes (vue Gold)
- `GET /api/routes/top` - Top lignes les plus actives

### 🚉 GTFS - Gares (Stops)

- `GET /api/stops` - Liste des gares (avec recherche)
- `GET /api/stops/{stop_id}` - Détails d'une gare
- `GET /api/stops/popularity` - Gares les plus fréquentées
- `GET /api/stops/top` - Top gares

### 🎫 GTFS - Trajets (Trips)

- `GET /api/trips` - Liste des trajets
- `GET /api/trips/{trip_id}` - Détails d'un trajet
- `GET /api/trips/{trip_id}/enriched` - Trajet enrichi (vue Gold)
- `GET /api/trips/{trip_id}/stops` - Arrêts d'un trajet
- `GET /api/trips/longest` - Trajets les plus longs

### 🌙 Back-on-Track - Trains de nuit

- `GET /api/night-trains` - Liste des trains de nuit
- `GET /api/night-trains/{routeid}` - Détails d'un train de nuit
- `GET /api/night-trains/{routeid}/enriched` - Train enrichi (vue Gold)
- `GET /api/night-trains/{routeid}/stations` - Gares d'un train de nuit
- `GET /api/night-trains/international` - Trains les plus internationaux

### 🔍 Recherche et Statistiques

- `GET /api/search` - Recherche globale
- `GET /api/stats` - Statistiques globales
- `GET /health` - État de l'API

## 📖 Exemples d'Utilisation

### Rechercher une gare

```bash
curl "http://localhost:8000/api/stops?search=Paris&limit=5"
```

### Obtenir les top lignes

```bash
curl "http://localhost:8000/api/routes/top?limit=10"
```

### Obtenir les détails d'un trajet

```bash
curl "http://localhost:8000/api/trips/OCESN105342F6957299:2025-12-08T01:44:48Z/enriched"
```

### Recherche globale

```bash
curl "http://localhost:8000/api/search?q=Lyon&limit=10"
```

## 🎯 Fonctionnalités

- ✅ **Documentation automatique** : Swagger UI et ReDoc
- ✅ **Validation des données** : Modèles Pydantic
- ✅ **Gestion d'erreurs** : Codes HTTP appropriés
- ✅ **Recherche** : Filtres et recherche textuelle
- ✅ **Pagination** : Limites de résultats
- ✅ **Vues Gold** : Utilisation des données agrégées pour performance

## 🔧 Configuration

L'API utilise la configuration dans `etl/config/config.py` pour se connecter à PostgreSQL.

## 📊 Structure

```
etl/api/
├── __init__.py
├── main.py          # Application FastAPI principale
├── models.py        # Modèles Pydantic
├── database.py      # Connexion à la base de données
└── README.md        # Ce fichier
```

