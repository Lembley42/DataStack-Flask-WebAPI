# Flask
from flask import Blueprint, request, jsonify
# General
from bson.objectid import ObjectId
from datetime import datetime, timedelta
# Services
from services.mongodb import Get_Database
# Helpers
from helpers.jsonencoder import Get_Encoder


li_fq_bp = Blueprint('linkedin_frequency', __name__)
data_db = Get_Database('linkedin_frequenz')

# Upload
@li_fq_bp.route('/upload/linkedin_frequency/<string:customer_name>', methods=['POST'])
def upload_linkedin_frequency(customer_name):
    if request.method == 'POST':
        collection = data_db[customer_name]
        data = request.get_json()
        # Change date to datetime object
        data['date'] = datetime.strptime(data['date'], '%Y-%m-%d')
        # Insert data into database if doesn't exist, else update existing data
        collection.find_one_and_update({'date': data['date'], 'campaign_id': data['campaign_id']}, {'$set': {'impressions': data['impressions'], 'reach': data['reach']}}, upsert=True)
        return 'Data uploaded successfully', 200
