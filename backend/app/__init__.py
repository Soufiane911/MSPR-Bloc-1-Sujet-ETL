import os
from flask import Flask
from flask_cors import CORS
from .db import db, initDb
from . import models
from prometheus_flask_exporter import PrometheusMetrics

metrics = PrometheusMetrics.for_app_factory()


def createApp():
    app = Flask(__name__)
    CORS(app)

    app.config["APP_ENV"] = os.getenv("FLASK_ENV", "production")
    app.config["ALLOW_INITDB"] = os.getenv("ALLOW_INITDB", "false").lower() == "true" or app.config["APP_ENV"] in {"development", "testing"}

    initDb(app)

    metrics.init_app(app)

    from .routes import api
    app.register_blueprint(api)

    return app
