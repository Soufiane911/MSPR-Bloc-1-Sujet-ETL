from flask import Flask
from flask_cors import CORS
from .db import db, init_db

def create_app():
    app = Flask(__name__)
    CORS(app)

    init_db(app)

    from .routes import api
    app.register_blueprint(api)

    return app
