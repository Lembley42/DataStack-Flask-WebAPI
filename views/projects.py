# Flask
from flask import Blueprint, request, jsonify
# General
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import json
# Services
from services.mongodb import Get_Database
# Helpers
from helpers.jsonencoder import Get_Encoder


projects_bp = Blueprint('projects', __name__)
projects_db = Get_Database('projects')


@projects_bp.route('/projects/create', methods=['POST'])
def create_project():
    if request.method == 'POST':
        collection = projects_db['all']
        data = request.get_json()
        user = data['user']
        project = data['project']
        collection.insert_one(project)
        # Add user to project / Project to user
        return 'Project created successfully', 200


@projects_bp.route('/projects/read/<string:project_id>', methods=['GET'])
def read_project(project_id):
    if request.method == 'GET':
        collection = projects_db['all']
        project = collection.find_one({'_id': ObjectId(project_id)})
        json_data = json.dumps(project, cls=Get_Encoder())
        return json_data, 200


@projects_bp.route('/projects/read/byUser/<string:user_id>', methods=['GET'])
def read_user_projects(user_id):
    if request.method == 'GET':
        collection = projects_db['all']
        projects = list(collection.find({'users': {'$in': [user_id]}}))
        json_data = json.dumps(projects, cls=Get_Encoder())
        return json_data, 200

