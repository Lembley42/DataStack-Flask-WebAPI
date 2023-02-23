# Flask
from flask import Blueprint, request, jsonify
# General
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import json, croniter
# Services
from services.mongodb import Get_Database
# Helpers
from helpers.jsonencoder import Get_Encoder


tasks_bp = Blueprint('tasks', __name__)
tasks_db = Get_Database('userData')
tasks_coll = tasks_db['tasks']

@tasks_bp.route('/tasks/create', methods=['POST'])
def create_task(task_type):
    if request.method == 'POST':
        tasks_coll.insert_one(request.get_json())
        return 'Task created successfully', 200
    else: return 'Wrong request method, expected POST', 400


@tasks_bp.route('/tasks/read/<string:task_id>', methods=['GET'])
def read_task(task_id):
    if request.method == 'GET':
        task = tasks_coll.find_one({'_id': ObjectId(task_id)})
        json_data = json.dumps(task, cls=Get_Encoder())
        return json_data, 200


@tasks_bp.route('/tasks/update/<string:task_id>', methods=['PUT'])
def update_task(task_id):
    if request.method == 'PUT':
        tasks_coll.update_one({'_id': ObjectId(task_id)}, {'$set': request.get_json()})
        return 'Task updated successfully', 200


@tasks_bp.route('/tasks/delete/<string:task_id>', methods=['DELETE'])
def delete_task(task_id):
    if request.method == 'DELETE':
        tasks_coll.delete_one({'_id': ObjectId(task_id)})
        return 'Task deleted successfully', 200


@tasks_bp.route('/tasks/list', methods=['GET'])
def list_tasks():
    if request.method == 'GET':
        tasks = list(tasks_coll.find())
        json_data = json.dumps(tasks, cls=Get_Encoder())
        return (json_data), 200


@tasks_bp.route('/tasks/query', methods=['POST'])
def query_tasks(task_type):
    if request.method == 'POST':
        tasks = list(tasks_coll.find(request.get_json()))
        json_data = list(json.dumps(tasks, cls=Get_Encoder()))
        return json_data, 200

@tasks_bp.route('/tasks/log/<string:task_id>', methods=['PUT'])
def log_task(task_id):
    if request.method == 'PUT':
        tasks_coll.update_one({'_id': ObjectId(task_id)}, {'$push': {'log': request.get_json()}})
        return 'Task logged successfully', 200


@tasks_bp.route('/tasks/reschedule/<string:task_type>/<string:task_id>', methods=['PUT'])
def reschedule_task(task_type, task_id):
    if request.method == 'PUT':
        # Load task from database
        collection = tasks_db[task_type]
        task = collection.find_one({'_id': ObjectId(id)})
        # Get variables from task document
        mode = task['mode']
        next_run = task['schedule']['next_run']
        #TODO: Ensure that next_run is a datetime object on MongoDB
        if isinstance(next_run, str): next_run = datetime.strptime(task['schedule']['next_run'], '%Y-%m-%d %H:%M:%S') 
        elif isinstance(next_run, (int, float)): next_run = datetime.fromtimestamp(task['schedule']['next_run'])
        elif isinstance(next_run, datetime): next_run = task['schedule']['next_run']
        cron_string = task['schedule'][f'cron_{mode}']
        # Increase next_run by cron schedule
        next_run = croniter.croniter(cron_string, next_run).get_next(datetime)
        # If next run is still in the past, increase to the next round 5 minutes
        if next_run < datetime.now():
            next_run = next_run + timedelta(minutes=5 - next.minute % 5)
            next_run = next_run.replace(second=0, microsecond=0)
        # Update task
        collection.update_one({'_id': ObjectId(id)}, {'$set': {'schedule.next_run': next_run}})
        # Return success message
        return 'Task rescheduled successfully', 200


# Get Date Range
@tasks_bp.route('/tasks/date_range/<string:task_type>/<string:task_id>', methods=['GET'])
def get_date_range(task_type, task_id):
    if request.method == 'GET':
        # Get task from document by ObjectId
        collection = tasks_db[task_type]
        task = collection.find_one({'_id': ObjectId(task_id)})
        # Determine date range
        mode = task['mode']
        task_settings = task['settings']
        daysToLoad = task_settings['days_per_load']
        daysToUpdate = task_settings['days_per_update']
        first_date = datetime.strptime(task_settings['first_date'], '%Y-%m-%d')
        last_date = datetime.strptime(task_settings['last_date'], '%Y-%m-%d')
        today = datetime.utcnow()

        # If last date is within update range, ensure mode is update
        if mode == 'load' and (last_date + timedelta(days=daysToUpdate)) >= today:
            mode = 'update'
            collection.update_one({'_id': ObjectId(task_id)}, {'$set': {'mode': mode}})
        # If last date is not within update range, ensure mode is load
        elif mode == 'update' and (last_date + timedelta(days=daysToUpdate)) < today: 
            mode = 'load'
            collection.update_one({'_id': ObjectId(task_id)}, {'$set': {'mode': mode}})

        # When Update, start date is today and end date is today - daysToUpdate, but no earlier than first_date
        if mode == 'update':
            end_date = today
            start_date = end_date - timedelta(days=daysToUpdate)
            if end_date < first_date: end_date = first_date
        
        # When Load, start date is last_date and end date is last_date + daysToLoad, but no later than today
        elif mode == 'load':
            start_date = last_date
            end_date = start_date + timedelta(days=daysToLoad)
            if end_date > today: end_date = today
        
        # Convert to string with format #YYYY-MM-DD
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')

        # Return date range
        return jsonify({'start_date': start_date, 'end_date': end_date})



@tasks_bp.route('/tasks/block/<string:task_type>/<string:task_id>', methods=['PUT'])
def block_task(task_type, task_id):
    if request.method == 'PUT':
        collection = tasks_db[task_type]
        collection.update_one({'_id': ObjectId(task_id)}, {'$set': {'status': 'running'}})
        return 'Task blocked successfully', 200


@tasks_bp.route('/tasks/unblock/<string:task_type>/<string:task_id>', methods=['PUT'])
def unblock_task(task_type, task_id):
    if request.method == 'PUT':
        collection = tasks_db[task_type]
        collection.update_one({'_id': ObjectId(task_id)}, {'$set': {'status': 'idle'}})
        return 'Task unblocked successfully', 200



