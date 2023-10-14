from flask import Blueprint, jsonify
from src.app.mongo_connection import db
from bson import json_util

get_members_router = Blueprint('members', __name__)
get_tags_router = Blueprint('tags', __name__)


def register_generals_routes(app):
    app.register_blueprint(get_members_router, url_prefix='/generals')
    app.register_blueprint(get_tags_router, url_prefix='/generals')


@get_members_router.route('/members', methods=['GET'])
def get_members():
    members = db.users.find({})
    formatted_members = [
        {**member, '_id': str(member['_id'])} for member in members
    ]

    members_json = json_util.dumps(formatted_members)

    return jsonify(members_json)


@get_tags_router.route('/tags', methods=['GET'])
def get_tags():
    tags = ["Frontend", "Backend", "Infra"]

    return jsonify(tags)
