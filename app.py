### IMPORTS ###
from flask import Flask, request, jsonify, Blueprint
import os 
# Helper Imports
from helpers.filedecryption import Decrypt_File
from helpers.jsonencoder import Get_Encoder
# View Imports
from views.tasks import tasks_bp
from views.linkedin_frequency import li_fq_bp

# Create Flask app
app = Flask(__name__)

# Get Environment Variables
API_KEY = os.environ.get('API_KEY')
GOOGLE_PROJECT_ID = os.environ.get('GOOGLE_PROJECT_ID')

# Set Google credentials
Decrypt_File('encrypted_files/google-credentials.bin', 'google-credentials.json', os.environ.get('GOOGLE_CREDENTIALS_KEY'))
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'google-credentials.json'

# Register JSON Encoder
JSONEncoder = Get_Encoder()

# Register Blueprints
for bp in [tasks_bp, li_fq_bp]:
    app.register_blueprint(bp)

### BEFORE REQUEST ###
@app.before_request
def check_api_key():
    if request.args.get('api_key') != API_KEY:
        return jsonify({'status': 'error', 'message': 'Invalid API key'}), 403
    

if __name__ == '__main__':
    app.run(debug=True)
