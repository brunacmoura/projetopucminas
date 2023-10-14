from flask import Flask
from flask_session import Session
from flask_cors import CORS
from src.app.routes.projects_router import (
    register_projects_routes)
from src.app.routes.generals_router import register_generals_routes
from src.app.routes.auths_router import register_auth_routes
from src.app.routes.tasks_router import register_tasks_routes

app = Flask(__name__)
CORS(app, origins="*")
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)


register_auth_routes(app)
register_projects_routes(app)
register_generals_routes(app)
register_tasks_routes(app)


if __name__ == '__main__':
    app.run(debug=True)
