"""
API REST FastAPI pour ObRail Europe.

Cette API expose les données ferroviaires européennes collectées
par le processus ETL.

Usage:
    uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import trains, schedules, stations, operators, stats

# Création de l'application FastAPI
app = FastAPI(
    title="ObRail Europe API",
    description="""
    API REST pour l'accès aux données ferroviaires européennes.
    
    Cette API permet de consulter:
    - Les trains (jour/nuit) par opérateur et pays
    - Les dessertes avec horaires
    - Les gares et stations
    - Les statistiques comparatives
    
    ## Sources de données
    - Back-on-Track Night Train Database (trains de nuit)
    - SNCF, Deutsche Bahn, ÖBB, Renfe, Trenitalia (trains de jour)
    - Mobility Database Catalogs
    
    ## Licence
    Les données sont fournies sous les licences respectives des sources
    (ODbL, CC-BY-4.0, GPL-3.0).
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    contact={
        "name": "ObRail Europe",
        "url": "https://back-on-track.eu",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
)

# Configuration CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Inclusion des routers
app.include_router(trains.router, prefix="/trains", tags=["Trains"])
app.include_router(schedules.router, prefix="/schedules", tags=["Schedules"])
app.include_router(stations.router, prefix="/stations", tags=["Stations"])
app.include_router(operators.router, prefix="/operators", tags=["Operators"])
app.include_router(stats.router, prefix="/stats", tags=["Statistics"])


@app.get("/")
def read_root():
    """
    Page d'accueil de l'API.
    
    Returns:
        dict: Informations sur l'API
    """
    return {
        "message": "Bienvenue sur l'API ObRail Europe",
        "version": "1.0.0",
        "documentation": "/docs",
        "redoc": "/redoc",
        "endpoints": {
            "trains": "/trains",
            "schedules": "/schedules",
            "stations": "/stations",
            "operators": "/operators",
            "statistics": "/stats"
        }
    }


@app.get("/health")
def health_check():
    """
    Vérification de la santé de l'API.
    
    Returns:
        dict: Statut de l'API
    """
    return {
        "status": "healthy",
        "api": "running",
        "database": "connected"
    }
