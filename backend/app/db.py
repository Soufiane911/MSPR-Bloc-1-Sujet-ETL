import os
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def _database_uri() -> str:
    """Resolve the database URI with a sensible local fallback for dev."""

    return os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/appdb")


def init_db(app):
    app.config["SQLALCHEMY_DATABASE_URI"] = _database_uri()
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)
