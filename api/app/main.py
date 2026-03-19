from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from app.routers import trains, schedules, stations, operators, stats
from app.middleware import PrometheusMiddleware

app = FastAPI(
    title="ObRail Europe API",
    description=(
        "API REST pour l'accès aux données ferroviaires européennes.\n\n"
        "Cette API permet de consulter:\n"
        "- Les trains (jour/nuit) par opérateur et pays\n"
        "- Les dessertes avec horaires\n"
        "- Les gares et stations\n"
        "- Les statistiques comparatives\n\n"
        "## Sources de données\n"
        "- Back-on-Track Night Train Database (trains de nuit)\n"
        "- SNCF, Deutsche Bahn, ÖBB, Renfe, Trenitalia (trains de jour)\n"
        "- Mobility Database Catalogs\n\n"
        "## Licence\n"
        "Les données sont fournies sous les licences respectives des sources\n"
        "(ODbL, CC-BY-4.0, GPL-3.0)."
    ),
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

app.add_middleware(PrometheusMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(trains.router, prefix="/trains", tags=["Trains"])
app.include_router(schedules.router, prefix="/schedules", tags=["Schedules"])
app.include_router(stations.router, prefix="/stations", tags=["Stations"])
app.include_router(operators.router, prefix="/operators", tags=["Operators"])
app.include_router(stats.router, prefix="/stats", tags=["Statistics"])


@app.get("/")
def read_root():
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
    return {
        "status": "healthy",
        "api": "running",
        "database": "connected"
    }


@app.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
        headers={"Content-Type": CONTENT_TYPE_LATEST}
    )
