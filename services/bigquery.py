from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime, date, time
import json



# Set up the BigQuery client
client = None

def Connect():
    global client
    if client is None:
        print('Connecting to BigQuery...')
        client = bigquery.Client()
        print('Connected to BigQuery')
    return client


def Map_Bigquery_Types(json_obj):
    bigquery_types = {}
    for key, value in json_obj.items():
        if isinstance(value, int):
            bigquery_types[key] = 'INT64'
        elif isinstance(value, float):
            bigquery_types[key] = 'FLOAT64'
        elif isinstance(value, bool):
            bigquery_types[key] = 'BOOL'
        elif isinstance(value, str):
            bigquery_types[key] = 'STRING'
        elif isinstance(value, bytes):
            bigquery_types[key] = 'BYTES'
        elif isinstance(value, date):
            bigquery_types[key] = 'DATE'
        elif isinstance(value, time):
            bigquery_types[key] = 'TIME'
        elif isinstance(value, datetime):
            bigquery_types[key] = 'DATETIME'
        elif isinstance(value, (list, tuple)):
            if len(value) > 0:
                # recursively call get_bigquery_types on the first element of the list or tuple
                bigquery_types[key] = Map_Bigquery_Types(value[0])
            else:
                bigquery_types[key] = 'STRING'
        elif isinstance(value, dict):
            # recursively call get_bigquery_types on the dictionary
            bigquery_types[key] = Map_Bigquery_Types(value)
        else:
            bigquery_types[key] = 'STRING'
    return bigquery_types


def Upload_JSON(dataset_name, table_name, json_list):
    client = Connect()

    # Check if the dataset exists, if not create it
    dataset_ref = client.dataset(dataset_name)
    if not client.get_dataset(dataset_ref):
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        print(f"Created dataset {dataset.dataset_id}")

    # Check if the table exists, if not create it with the schema based on the JSON list
    table_ref = dataset_ref.table(table_name)
    try:
        table = client.get_table(table_ref)
    except NotFound:
        table_schema = bigquery.Schema.from_dict(Map_Bigquery_Types(json_list[0]))
        table = bigquery.Table(table_ref, schema=table_schema)
        table = client.create_table(table)
        print(f"Created table {table.table_id}")

    errors = client.insert_rows(table, json_list)

    if errors == []: print('Data successfully inserted into the target table.')
    else: 
        print('Encountered errors while inserting data into the target table.')
        print(errors)
