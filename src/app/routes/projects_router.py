from datetime import datetime
from flask import Blueprint, abort, jsonify, request
from src.app.mongo_connection import db
from bson import ObjectId, DBRef, json_util
import re


get_projects_router = Blueprint('projects', __name__)
get_project_by_page_router = Blueprint('project_by_page', __name__)
create_project_router = Blueprint('create_project', __name__)


def register_projects_routes(app):
    app.register_blueprint(get_projects_router, url_prefix='/projects')
    app.register_blueprint(get_project_by_page_router, url_prefix='/projects')
    app.register_blueprint(create_project_router, url_prefix='/projects')


def serialize(obj):
    if isinstance(obj, ObjectId):
        return str(obj)
    elif isinstance(obj, DBRef):
        return {'$ref': obj.collection, '$id': str(obj.id)}
    elif isinstance(obj, list):
        return [serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: serialize(value) for key, value in obj.items()}
    return obj


@get_projects_router.route('/', methods=['GET'])
def get_projects():
    projects = db.projects.find({}, {'_id': 0})
    serialized_projects = json_util.dumps(list(projects))
    return serialized_projects


@get_project_by_page_router.route('/<page>', methods=['GET'])
def get_project_by_page(page: str):
    project = db.projects.find_one({'page': page})
    if project:
        serialized_project = json_util.dumps(project)
        return serialized_project
    else:
        abort(404, description="Project not found")


@create_project_router.route('/create', methods=['POST'])
def create_project():
    data = request.json
    page_value = data['titleCtrl'].lower().replace(' ', '_')
    project_name = data["titleCtrl"]
    existing_project = db.projects.find_one(
        {'name': {'$regex': f'^{re.escape(project_name)}$', '$options': 'i'}})
    if existing_project:
        return jsonify({'code': 400,
                        'error': 'Another project with this title was found.'})
    new_project = {
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow(),
        'name': project_name,
        'page': page_value,
        'description': data['descriptionCtrl'],
        'members': [DBRef('users',
                    ObjectId(user_id)) for user_id in data['membersCtrl']],
        'status': data['statusCtrl'],
        'owner': DBRef('users', data['owner']) if 'owner' in data
        else DBRef('users', '6526e7ef18e97ad0e8159489')
    }

    db.projects.insert_one(new_project)

    return jsonify({'code': 200, 'message': 'Project created'})
