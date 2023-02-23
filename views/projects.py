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
projects_db = Get_Database('userData')
projects_coll = projects_db['projects']

@projects_bp.route('/projects/create', methods=['POST'])
def create_project():
    if request.method == 'POST':
        data = request.get_json()
        user = data['user']
        project = data['project']
        projects_coll.insert_one(project)
        #TODO: Add user to project / Project to user
        return 'Project created successfully', 200
    else: return 'Wrong request method, expected POST', 400


@projects_bp.route('/projects/read/<string:project_id>', methods=['GET'])
def read_project(project_id):
    if request.method == 'GET':
        project = projects_coll.find_one({'_id': ObjectId(project_id)})
        json_data = json.dumps(project, cls=Get_Encoder())
        return json_data, 200
    else: return 'Wrong request method, expected GET', 400


@projects_bp.route('/projects/update/<string:project_id>', methods=['PUT'])
def update_project(project_id):
    if request.method == 'PUT':
        data = request.get_json()
        project = data['project']
        projects_coll.update_one({'_id': ObjectId(project_id)}, {'$set': project})
        return 'Project updated successfully', 200
    else: return 'Wrong request method, expected PUT', 400


@projects_bp.route('/projects/delete/<string:project_id>', methods=['DELETE'])
def delete_project(project_id):
    if request.method == 'DELETE':
        projects_coll.delete_one({'_id': ObjectId(project_id)})
        return 'Project deleted successfully', 200
    else: return 'Wrong request method, expected DELETE', 400


@projects_bp.route('/projects/read/byUser/<string:user_id>', methods=['GET'])
def read_user_projects(user_id):
    if request.method == 'GET':
        projects = list(projects_coll.find({'users': {'$in': [user_id]}}))
        json_data = json.dumps(projects, cls=Get_Encoder())
        return json_data, 200
    else: return 'Wrong request method, expected GET', 400
