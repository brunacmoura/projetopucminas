from datetime import datetime
from flask import Blueprint, abort, jsonify, request
from src.app.mongo_connection import db
from bson import ObjectId, DBRef


get_tasks_by_project_router = Blueprint('tasks', __name__)
create_task_router = Blueprint('create_task', __name__)
update_task_status_router = Blueprint('update_task_status', __name__)
remove_task_router = Blueprint('remove_task', __name__)
update_task_router = Blueprint('update_task', __name__)


def register_tasks_routes(app):
    app.register_blueprint(get_tasks_by_project_router, url_prefix='/tasks')
    app.register_blueprint(create_task_router, url_prefix='/tasks')
    app.register_blueprint(update_task_status_router, url_prefix='/tasks')
    app.register_blueprint(remove_task_router, url_prefix='/tasks')
    app.register_blueprint(update_task_router, url_prefix='/tasks')


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


@get_tasks_by_project_router.route('/<project_id>', methods=['GET'])
def get_tasks_by_project(project_id: str):
    project = db.projects.find_one({'_id': ObjectId(project_id)})
    if project:
        project_id = project["_id"]

        pipeline = [
            {"$match": {"project_id.$id": project_id}},
            {"$unwind": "$owner"},
            {"$lookup": {
                "from": "users",
                "localField": "owner.$id",
                "foreignField": "_id",
                "as": "ownerInfo"
            }},
            {"$unwind": "$ownerInfo"},
            {"$addFields": {"owner_info": "$ownerInfo.name"}},
            {"$group": {"_id": "$status", "tasks": {"$push": "$$ROOT"}}},
            {"$project": {
                "_id": 1,
                "tasks": {
                    "_id": 1,
                    "title": 1,
                    "description": 1,
                    "owner": 1,
                    "status": 1,
                    "delivery_date": 1,
                    "project_id": 1,
                    "tag": 1,
                    "owner_info": 1
                }
            }}
        ]

        result = list(db.tasks.aggregate(pipeline))
        result = serialize(result)

        return jsonify(result)
    else:
        abort(404, description="Project not found")


@create_task_router.route('/create', methods=['POST'])
def create_task():
    data = request.json
    responsible_id = data['responsibleCtrl']
    project_id = data['projectId']
    date_string = data["deliveryDateCtrl"]
    if 'owner' in data:
        user_id = data["owner"]
    else:
        user_id = None

    if date_string:
        date_object = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%fZ')

    new_task = {
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow(),
        'title': data['titleCtrl'],
        'description': data['descriptionCtrl'],
        'owner': DBRef('users', ObjectId(responsible_id)),
        'status': "Backlog",
        'delivery_date': date_object if date_string else None,
        'project_id': DBRef('projects', ObjectId(project_id)),
        'tag': data['tagCtrl']
    }

    result = db.tasks.insert_one(new_task)

    if result.acknowledged:
        if user_id is not None:
            task_id = result.inserted_id
            log = {
                "created_at": datetime.utcnow(),
                "request": 'create',
                "owner": DBRef('users', ObjectId(user_id)),
                "details": data['titleCtrl'],
                "task_id": task_id
            }
            db.task_logs.insert_one(log)
        return jsonify({"success": True,
                        'message': 'Task created sucessfully'})
    else:
        return jsonify({"success": False, "error": "Failed to create task"})


@update_task_router.route('/update/<task_id>', methods=['PUT'])
def update_task(task_id: str):
    data = request.json

    if 'owner' in data:
        user_id = data["owner"]
    else:
        user_id = None
    data = data["changes"]

    update_fields = {}

    if 'titleCtrl' in data:
        update_fields['title'] = data['titleCtrl']

    if 'descriptionCtrl' in data:
        update_fields['description'] = data['descriptionCtrl']

    if 'responsibleCtrl' in data:
        responsible = data['responsibleCtrl']
        if responsible is not None:
            update_fields['owner'] = DBRef('users', ObjectId(responsible))
        else:
            update_fields['owner'] = None

    if 'tagCtrl' in data:
        update_fields['tag'] = data['tagCtrl']

    if 'deliveryDateCtrl' in data:
        date_string = data["deliveryDateCtrl"]
        update_fields['delivery_date'] = datetime.strptime(
            date_string, '%Y-%m-%dT%H:%M:%S.%fZ') if date_string else None

    update_fields['updated_at'] = datetime.utcnow()

    result = db.tasks.find_one_and_update(
        {'_id': ObjectId(task_id)},
        {'$set': update_fields}
    )

    if result:
        if user_id is not None:
            task = db.tasks.find_one({'_id': ObjectId(task_id)})
            title = task.get('title')
            log = {
                "created_at": datetime.utcnow(),
                "request": 'update',
                "owner": DBRef('users', ObjectId(user_id)),
                "details": title,
                "task_id": task_id
            }
            db.task_logs.insert_one(log)
        return jsonify({'message': 'Task updated sucessfully'})
    else:
        return jsonify({'error': 'Task not found'})


@update_task_status_router.route('/update_status/<task_id>', methods=['PUT'])
def update_task_status(task_id: str):
    data = request.json
    new_status = data["newStatus"]["newStatus"]

    if 'owner' in data:
        user_id = data["owner"]
    else:
        user_id = None

    if not new_status:
        return jsonify({'error': 'New status was not defined'}), 400

    task = db.tasks.find_one({'_id': ObjectId(task_id)})
    if not task:
        return jsonify({'error': 'Task not found'}), 404

    result = db.tasks.update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {'status': new_status}}
    )
    if result:
        if user_id is not None:
            title = task.get('title')
            log = {
                "created_at": datetime.utcnow(),
                "request": 'update-status',
                "owner": DBRef('users', ObjectId(user_id)),
                "details": title,
                "task_id": task_id
            }
            db.task_logs.insert_one(log)
        return jsonify({'message': 'Task status updated sucessfully'})
    return jsonify({'message': 'Error updating task status'})


@remove_task_router.route('/remove/<task_id>', methods=['DELETE'])
def remove_task(task_id: str):
    user_id = request.args.get('owner')

    task = db.tasks.find_one({'_id': ObjectId(task_id)})
    result = db.tasks.delete_one({'_id': ObjectId(task_id)})

    if result.deleted_count == 1:
        if user_id is not None:
            title = task.get('title')
            log = {
                "created_at": datetime.utcnow(),
                "request": 'delete',
                "owner": DBRef('users', ObjectId(user_id)),
                "details": title,
                "task_id": task_id
            }
            db.task_logs.insert_one(log)
        return jsonify({'message': 'Task removed'})
    else:
        return jsonify({'error': 'Task not found'})
