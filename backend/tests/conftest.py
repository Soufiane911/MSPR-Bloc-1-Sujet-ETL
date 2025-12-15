import os
import shutil
import tempfile
import pytest

from app import create_app
from app.db import db


@pytest.fixture(scope="function")
def app():
    # Temporary SQLite DB file per test for isolation and persistence across connections
    tmp_dir = tempfile.mkdtemp(prefix="gtfs_test_")
    db_path = os.path.join(tmp_dir, "test.db")
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    os.environ["FLASK_ENV"] = "testing"
    os.environ["ALLOW_INITDB"] = "true"

    app = create_app()

    with app.app_context():
        db.create_all()

    yield app

    # Teardown
    with app.app_context():
        db.session.remove()
        db.drop_all()
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture()
def client(app):
    return app.test_client()
