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
