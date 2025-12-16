import os
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def _databaseUri() -> str:
    return os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/appdb")


def initDb(app):
    app.config["SQLALCHEMY_DATABASE_URI"] = _databaseUri()
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)
