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
4. une couche d'exposition composee d'une API FastAPI et d'un dashboard Streamlit.

```text
Sources de donnees
    -> ETL Python (extraction, nettoyage, normalisation, classification, chargement)
    -> PostgreSQL
    -> API REST FastAPI
    -> Dashboard Streamlit
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

Le dashboard Streamlit fournit une interface de consultation des indicateurs de qualite, des comparaisons jour/nuit et des vues synthetiques par pays, operateur ou type de service.

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
2. Démarent Docker Desktop s'il ne l'est pas déjà
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

### Services exposes

| Service | Adresse | Description |
|---|---|---|
| API REST | `http://localhost:8000` | Service FastAPI |
| Documentation OpenAPI | `http://localhost:8000/docs` | Interface Swagger |
| Dashboard | `http://localhost:8501` | Interface Streamlit |
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

## Validation et tests

Le depot contient des tests unitaires cibles sur l'API et l'ETL.

```bash
pytest -q
```

Il est egalement possible d'executer les sous-ensembles de tests depuis les repertoires concernes selon les besoins de validation locale.

## Limites connues

- certaines sources GTFS peuvent evoluer ou devenir indisponibles sans preavis ;
- la qualification jour/nuit depend de la qualite et de la granularite des donnees sources ;
- certaines sources nationales necessitent un filtrage metier pour exclure les dessertes trop locales au regard de la problematique ;
- l'URL actuellement utilisee pour la Suisse doit etre verifiee regulierement afin de confirmer qu'elle reste directement exploitable dans le pipeline.

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
