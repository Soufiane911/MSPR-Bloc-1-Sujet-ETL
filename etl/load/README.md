# 💾 Chargement des Données dans PostgreSQL

## 📋 Prérequis

1. **PostgreSQL installé et démarré**
2. **Base de données créée** : `obrail_europe`
3. **Configuration** : Modifier `etl/config/config.py` avec vos identifiants

## 🚀 Utilisation

### Étape 1 : Créer le schéma (tables)

```bash
cd etl
python load/create_schema.py
```

Ce script :
- ✅ Se connecte à PostgreSQL
- ✅ Crée toutes les tables (10 tables)
- ✅ Crée les index et contraintes
- ✅ Crée les vues utiles

### Étape 2 : Charger les données

```bash
cd etl
python load/load_data.py
```

Ce script :
- ✅ Charge les données GTFS (6 tables)
- ✅ Charge les données Back-on-Track (4 tables)
- ✅ Utilise des chunks pour les gros fichiers
- ✅ Affiche une barre de progression

## ⚙️ Configuration

Modifiez `etl/config/config.py` :

```python
DATABASE = {
    "host": "localhost",
    "port": 5432,
    "name": "obrail_europe",
    "user": "postgres",
    "password": "votre_mot_de_passe"  # ← À compléter
}
```

Ou utilisez une variable d'environnement :
```bash
export POSTGRES_PASSWORD="votre_mot_de_passe"
```

## 📊 Ordre de Chargement

Les données sont chargées dans l'ordre correct pour respecter les foreign keys :

**GTFS :**
1. `agency` (7 lignes)
2. `routes` (798 lignes)
3. `stops` (8 878 lignes)
4. `calendar_dates` (318 756 lignes)
5. `trips` (48 971 lignes)
6. `stop_times` (429 422 lignes) ⚠️ Le plus gros

**Back-on-Track :**
1. `night_trains` (200 lignes)
2. `night_train_countries` (298 lignes)
3. `night_train_operators` (218 lignes)
4. `night_train_stations` (2 032 lignes)

## ⏱️ Temps Estimé

- **Création du schéma** : < 1 seconde
- **Chargement GTFS** : 2-5 minutes (selon la machine)
- **Chargement Back-on-Track** : < 10 secondes
- **Total** : ~3-6 minutes

## 🔍 Vérification

Après le chargement, vérifiez les données :

```sql
-- Compter les lignes
SELECT 'agency' as table_name, COUNT(*) as count FROM agency
UNION ALL
SELECT 'routes', COUNT(*) FROM routes
UNION ALL
SELECT 'stops', COUNT(*) FROM stops
UNION ALL
SELECT 'trips', COUNT(*) FROM trips
UNION ALL
SELECT 'stop_times', COUNT(*) FROM stop_times
UNION ALL
SELECT 'calendar_dates', COUNT(*) FROM calendar_dates
UNION ALL
SELECT 'night_trains', COUNT(*) FROM night_trains;
```

## ⚠️ En cas d'erreur

1. **Erreur de connexion** : Vérifiez que PostgreSQL est démarré
2. **Erreur de mot de passe** : Vérifiez `config.py`
3. **Erreur de table** : Exécutez d'abord `create_schema.py`
4. **Erreur de foreign key** : Vérifiez l'ordre de chargement

## 🔄 Réinitialisation

Pour tout recommencer :

```sql
-- Supprimer toutes les tables
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;
```

Puis réexécutez `create_schema.py` et `load_data.py`.

