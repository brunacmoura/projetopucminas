from flask import Blueprint, request, jsonify, session
from src.app.mongo_connection import db

auth_router = Blueprint('auth', __name__)


def register_auth_routes(app):
    app.register_blueprint(auth_router, url_prefix='/auth')


@auth_router.route('/login', methods=['POST'])
def login():
    data = request.json

    username = data.get('name')
    password = data.get('password')

    user = db.users.find_one({'name': username})
    if user and user['password'] == password:
        session['user_id'] = str(user['_id'])
        return jsonify({'message': 'Sucessfull Authentication',
                        'user': str(user['_id'])})
    else:
        return jsonify({'message': 'Invalid Credentials'}), 401


@auth_router.route('/logout', methods=['POST'])
def logout():
    session.pop('user_id', None)
    return jsonify({'message': 'Sucessfull Logout'})
