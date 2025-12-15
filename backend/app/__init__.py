import os
from flask import Flask
from flask_cors import CORS
from .db import db, init_db
from . import models  # noqa: F401 ensures models are registered before create_all

def create_app():
    app = Flask(__name__)
    CORS(app)

    app.config["APP_ENV"] = os.getenv("FLASK_ENV", "production")
    app.config["ALLOW_INITDB"] = os.getenv("ALLOW_INITDB", "false").lower() == "true" or app.config["APP_ENV"] in {"development", "testing"}

    init_db(app)

    from .routes import api
    app.register_blueprint(api)

    return app
