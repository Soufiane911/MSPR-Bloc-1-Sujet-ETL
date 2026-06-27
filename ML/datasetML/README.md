# Dataset ML ObRail

Ce dossier contient l'atelier de construction du dataset supervise utilise pour le modele de substitution avion -> train.

## Objectif

Construire un fichier propre et reutilisable pour l'entrainement du modele ML :

```text
data/ml/obrail_ml_dataset.csv
```

Le dossier `ML/datasetML/` garde les scripts, la requete SQL de reference et les sorties intermediaires afin de tracer la preparation des donnees.

## Structure

```text
ML/datasetML/
|-- build_dataset.py
|-- clean_dataset.py
|-- config.py
|-- extract_data.py
|-- feature_engineering.py
|-- validate_dataset.py
|-- sql/
|   `-- extract_obrail_routes.sql
`-- outputs/
    |-- raw_obrail_extract.csv
    |-- prepared_obrail_dataset.csv
    |-- data_quality_report.json
    `-- class_distribution.csv
```

## Variables principales

Le dataset final contient notamment :

- `distance_km`
- `duration_min`
- `duration_hours`
- `avg_speed_kmh`
- `is_international`
- `train_type`
- `weekly_frequency`
- `estimated_co2_saving_kg`
- `origin_country`
- `destination_country`
- `substitution_potential`

La cible `substitution_potential` prend trois valeurs :

- `faible_potentiel`
- `potentiel_moyen`
- `fort_potentiel`

## Execution

Depuis la racine du repo :

```bash
python -m ML.datasetML.build_dataset --source auto
```

Modes disponibles :

- `--source postgres` : extrait depuis PostgreSQL avec `sql/extract_obrail_routes.sql`.
- `--source local` : extrait depuis les fichiers locaux `data/raw/`.
- `--source auto` : tente PostgreSQL puis bascule sur `data/raw/` si la base n'est pas disponible.

## Sorties

Le pipeline ecrit :

- `data/ml/obrail_ml_dataset.csv` : livrable final attendu.
- `ML/datasetML/outputs/raw_obrail_extract.csv` : extraction brute consolidee.
- `ML/datasetML/outputs/prepared_obrail_dataset.csv` : copie du dataset prepare.
- `ML/datasetML/outputs/data_quality_report.json` : rapport qualite.
- `ML/datasetML/outputs/class_distribution.csv` : distribution des classes.

## Limites de cette premiere version

- La cible est construite par regles metier, faute de labels historiques observes.
- La frequence hebdomadaire est estimee depuis `calendar.txt`, `calendar_dates.txt` ou les libelles Back-on-Track.
- Sans base PostgreSQL active, l'extraction locale depuis `data/raw/` sert de fallback reproductible.
