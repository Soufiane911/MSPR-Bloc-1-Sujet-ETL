from flask import Blueprint, jsonify

api = Blueprint("api", __name__)

@api.get("/test")
def test():
    return jsonify({"message": "API is working"})
