import os
from pymongo import MongoClient
from bson.objectid import ObjectId

MONGO_PATH = os.environ.get('MONGO_PATH')
MONGO_USER = os.environ.get('MONGO_USER')
MONGO_PASS = os.environ.get('MONGO_PASS')

client = None


def Connect():
    global client
    if client is None:
        print('Connecting to MongoDB...')
        client = MongoClient(f'mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_PATH}/?retryWrites=true&w=majority')
        print('Connected to MongoDB')
    return client


def Get_Database(name):
    return Connect()[name]

