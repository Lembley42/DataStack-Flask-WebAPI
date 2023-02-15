from google.cloud import bigquery
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


def Upload_JSON(dataset_name, table_name, data):
    client = Connect()
    table_ref = client.dataset(dataset_name).table(table_name)
    table = client.get_table(table_ref)

    errors = client.insert_rows(table, data)

    if errors == []: print('Data successfully inserted into the target table.')
    else: print('Encountered errors while inserting data into the target table.')
