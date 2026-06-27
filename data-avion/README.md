# Donnees aviation - ObRail

Ce dossier contient des donnees aviation ouvertes pour enrichir le cas d'usage ObRail :
identifier des trajets aeriens europeens pour lesquels une alternative ferroviaire peut etre recommandee.

## Sources

### OurAirports

- Fichier importe : `raw/ourairports_airports.csv`
- Fichier importe : `raw/ourairports_countries.csv`
- Source : https://ourairports.com/data/
- Role : referentiel d'aeroports, codes IATA/ICAO, pays, villes et coordonnees.

### OpenFlights

- Fichier importe : `raw/openflights_airports.dat`
- Fichier importe : `raw/openflights_routes.dat`
- Source : https://openflights.org/data.php
- Role : routes aeriennes historiques entre aeroports.
- Limite importante : la base OpenFlights est utile pour une demonstration et un referentiel ouvert, mais ne doit pas etre presentee comme un inventaire temps reel des vols actuels.

## Fichiers prepares

| Fichier | Description | Usage recommande |
|---|---|---|
| `processed/europe_airports.csv` | Aeroports europeens avec service programme, code IATA, pays et coordonnees | Matching ville/aeroport |
| `processed/europe_air_routes_airline_level.csv` | Routes europeennes au niveau compagnie aerienne | Analyse detaillee |
| `processed/europe_air_routes.csv` | Routes europeennes origine-destination agregees | Interface utilisateur et matching train |
| `processed/europe_air_routes_demo.csv` | Echantillon de 300 routes internationales entre 250 et 1500 km | Demo MSPR |

## Volumetrie actuelle

- Aeroports europeens : 526
- Routes europeennes au niveau compagnie : 13 605
- Routes europeennes origine-destination uniques : 8 743
- Routes candidates de demo : 300

## Reproduction

Depuis la racine du projet `6.2-6.4` :

```bash
python data-avion/scripts/build_europe_air_data.py
```

Le script lit les fichiers bruts dans `raw/` et regenere les fichiers CSV dans `processed/`.

## Usage MSPR recommande

Pour la soutenance, ne pas promettre "tous les vols d'Europe en temps reel".

Formulation recommandee :

> Nous avons integre un referentiel ouvert de routes aeriennes europeennes afin de simuler la selection d'un trajet avion et d'evaluer, via le modele ObRail, le potentiel d'une alternative ferroviaire.

## Prochaines etapes possibles

1. Relier une route avion `origin_iata -> destination_iata` a une relation ferroviaire ObRail proche.
2. Calculer les variables d'entree du modele : distance, duree train, economie CO2 estimee, international, type de train.
3. Appeler le modele `/predict`.
4. Afficher le resultat dans l'interface : potentiel, confiance, temps train, economie CO2 et explication.

