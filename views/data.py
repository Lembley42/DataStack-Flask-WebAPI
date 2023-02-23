# Flask
from flask import Blueprint, request, jsonify
# General
from bson.objectid import ObjectId
from datetime import datetime, timedelta
import json
# Services
from services.mongodb import Get_Database
from services.bigquery import Upload_JSON
# Helpers
from helpers.jsonencoder import Get_Encoder

data_bp = Blueprint('data', __name__)
data_db = Get_Database('sourceData')
data_coll = data_db['projects']


@data_bp.route('/data/upload/mongodb/<task_type>', methods=['POST'])
def upload_data(task_type):
    if request.method == 'POST':
        collection = data_db[task_type]
        data = request.get_json()
        collection.insert_one(data)
        return 'Data uploaded successfully', 200
    else: return 'Wrong request method, expected POST', 400


@data_bp.route('/data/upload/bigquery/<task_type>', methods=['POST'])
def upload_data(task_type):
    if request.method == 'POST':
        data = request.get_json()
        # If data is a list, upload it as is, else upload it as a list
        if type(data) is list: Upload_JSON('sourceData', task_type, data)
        else: Upload_JSON('sourceData', task_type, [data])
        return 'Data uploaded successfully', 200
    else: return 'Wrong request method, expected POST', 400


@data_bp.route('/data/read/mongodb', methods=['POST'])
def query_data():
    if request.method == 'POST':
        query = request.get_json()
        collection = data_db[query['task_type']]
        data = collection.find(query['query'])
        json_data = json.dumps(data, cls=Get_Encoder())
        return json_data, 200
    else: return 'Wrong request method, expected POST', 400


