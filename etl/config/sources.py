"""
Configuration des sources de données pour l'ETL ObRail Europe.
"""

# Sources Tier 1 - Indispensables
SOURCES = {
    "back_on_track": {
        "name": "Back-on-Track Night Train Database",
        "type": "json_api",
        "base_url": "https://raw.githubusercontent.com/Back-on-Track-eu/night-train-data/main/data/latest",
        "endpoints": [
            "agencies",
            "stops",
            "routes",
            "trips",
            "trip_stop",
            "calendar",
            "calendar_dates",
            "translations",
            "classes"
        ],
        "country": "EU",
        "licence": "GPL-3.0",
        "priority": 1
    },
    "mobility_catalog": {
        "name": "Mobility Database Catalogs",
        "type": "csv",
        "url": "https://storage.googleapis.com/storage/v1/b/mdb-csv/o/sources.csv?alt=media",
        "country": "WW",
        "licence": "Apache-2.0",
        "priority": 1
    },
    "sncf_transilien": {
        "name": "SNCF Transilien",
        "type": "gtfs",
        "url": "https://eu.ftp.opendatasoft.com/sncf/gtfs/transilien-gtfs.zip",
        "country": "FR",
        "licence": "ODbL",
        "priority": 1
    },
    "sncf_intercites": {
        "name": "SNCF Intercités",
        "type": "gtfs",
        "url": "https://eu.ftp.opendatasoft.com/sncf/gtfs/export-intercites-gtfs-last.zip",
        "country": "FR",
        "licence": "ODbL",
        "priority": 1
    },
    "gtfs_de": {
        "name": "GTFS Allemagne",
        "type": "gtfs",
        "url": "https://download.gtfs.de/germany/rv_free/latest.zip",
        "country": "DE",
        "licence": "CC-BY-4.0",
        "priority": 1
    },
    "oebb": {
        "name": "ÖBB Autriche",
        "type": "gtfs",
        "url": "https://data.oebb.at/de/datensaetze~soll-fahrplan-gtfs~/download.csv",
        "country": "AT",
        "licence": "CC-BY-4.0",
        "priority": 1
    },
    "renfe": {
        "name": "Renfe Espagne",
        "type": "gtfs",
        "url": "https://ssl.renfe.com/gtransit/Fichero_AV_LD/google_transit.zip",
        "country": "ES",
        "licence": "CC-BY-4.0",
        "priority": 2
    },
    "trenitalia": {
        "name": "Trenitalia Italie",
        "type": "gtfs",
        "url": "http://www.sardegnamobilita.it/opendata/dati_trenitalia.zip",
        "country": "IT",
        "licence": "CC-BY-4.0",
        "priority": 2
    }
}

# Liste des pays européens pour filtrage
EUROPEAN_COUNTRIES = [
    'FR', 'DE', 'IT', 'ES', 'CH', 'AT', 'BE', 'NL', 'UK',
    'SE', 'NO', 'DK', 'FI', 'IE', 'PT', 'PL', 'CZ', 'HU',
    'RO', 'BG', 'HR', 'SI', 'SK', 'LT', 'LV', 'EE', 'LU'
]
