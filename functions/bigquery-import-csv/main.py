import os
import pdb
import sys
import json
import time
import uuid
import yaml
import base64
#import random
#import hashlib

#from datetime import datetime

#from google.cloud import pubsub
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import exceptions

#PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT', '')
#BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', '')

ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    BIGQUERY_DATASET = parsed_vars['BIGQUERY_DATASET']

    #PUBLISHER = pubsub.PublisherClient()
    CLIENT = bigquery.Client()

class BigQueryTable:

    def __init__(self, name, csv_uri, schema):
        """

        Args:
            name (str): Bigquery table config data
            csv_uri (str): Link to CSV source file
            schema_fields (dict): Key = column name & value = type, i.e. "STRING"
        """
        self.name = name
        self.csv_uri = csv_uri
        self.schema_fields = schema

    def write_csv_to_table(self):
        print(f"Loading table: {self.name}.")
        print(f"{BIGQUERY_DATASET} {PROJECT_ID}.")
        
        dataset_ref = CLIENT.dataset(BIGQUERY_DATASET)
        
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV 
        job_config.write_disposition = 'WRITE_TRUNCATE'

        bq_schema = []
        for label, kind in self.schema_fields.items():
            bq_schema.append(bigquery.SchemaField(label, kind))
        job_config.schema = bq_schema
        print(f"Table schema: {job_config.schema}")

        print(f"Job configuration: {job_config}.")
        load_job = CLIENT.load_table_from_uri(
                                    source_uris = self.csv_uri, 
                                    destination = dataset_ref.table(self.name), 
                                    job_config = job_config)
        print(f"Starting job {load_job.job_id}.")

        destination_table = CLIENT.get_table(dataset_ref.table(self.name))
        print(f"Loaded {destination_table.num_rows} rows.")

    def append_csv_to_table(self):
        print(f"Loading table: {self.name}.")
        print(f"{BIGQUERY_DATASET} {PROJECT_ID}.")
        
        dataset_ref = CLIENT.dataset(BIGQUERY_DATASET)
        
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV 
        job_config.write_disposition = 'WRITE_APPEND'

        bq_schema = []
        for label, kind in self.schema_fields.items():
            bq_schema.append(bigquery.SchemaField(label, kind))
        job_config.schema = bq_schema
        print(f"Table schema: {job_config.schema}")

        print(f"Job configuration: {job_config}.")
        load_job = CLIENT.load_table_from_uri(
                                    source_uris = self.csv_uri, 
                                    destination = dataset_ref.table(self.name), 
                                    job_config = job_config)
        print(f"Starting job {load_job.job_id}.")

        destination_table = CLIENT.get_table(dataset_ref.table(self.name))
        print(f"Loaded {destination_table.num_rows} rows.")


def import_csv_to_bigquery(event, context):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"> Context: {context}.")
    print(f"> Data: {data}.")
    header = data['header']
    body = data['body']

    # Get seed/event ID to track provenance of Trellis events
    seed_id = header['seedId']
    event_id = context.event_id

    node = body['results'].get('node')
    if not node:
        print("> No node provided. Exiting.")
        return

    required_labels = ['Blob']
    # Check whether node label is supported
    supported_labels = [
                       'Fastqc',
                       'Flagstat', 
                       'Vcfstats'
    ]
    
    conditions = [
        # Check that all required labels are present
        set(required_labels).issubset(set(node.get('labels'))),
        # Check that one & only one type format class is represented
        len(set(supported_labels).intersection(set(node.get('labels'))))==1,
    ]

    for condition in conditions:
        if condition:
            continue
        else:
            logging.error(f"Input node does not match requirements. Node: {node}.")
            return False

    task_label = set(supported_labels).intersection(set(node.get('labels'))).pop()

    # Get BigQuery table configurations
    with open('bigquery-config.json') as fh:
        bigquery_configs = json.load(fh)
    config_data = bigquery_configs[label]

    csv_bucket = node['csv_bucket']
    csv_path = node['csv_path']
    csv_uri = f"gs://{csv_bucket}/{csv_path}"
    
    # I don't know why I have this in a separate function...
    print(f"Loading object {csv_uri} into {config_data['table-name']} in {BIGQUERY_DATASET}.")
    bq_table = BigQueryTable(
                             name = config_data['table-name'], 
                             csv_uri = csv_uri,
                             schema = config_data['schema-fields'])
    try:
        bq_table.append_csv_to_table()
    except excpetions.NotFound:
        print(f"Table not found. Creating table.")
        dataset_ref = CLIENT.dataset(BIGQUERY_DATASET)
        CLIENT.create_table(bq_table)
        tables = list(CLIENT.list_tables(dataest_ref))
        print(f"Table created. Dataset tables: {tables}.")

# For local testing
if __name__ == "__main__":
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"
    BIGQUERY_DATASET = 'mvp_wgs_9000'
    CLIENT = bigquery.Client(project=PROJECT_ID)

    data = {
            'resource': 'node', 
            'neo4j-metadata': {
                               'node': {
                                        'extension': 'txt.csv', 
                                        'time-created-iso': '2019-04-02T23:09:30.066000+00:00', 
                                        'dirname': 'dsub/vcfstats/concat/objects', 
                                        'path': 'dsub/vcfstats/concat/objects/concat_vcfstats.txt.csv', 
                                        'storageClass': 'REGIONAL', 
                                        'md5Hash': '2lSvb8PhCk/nssCM3R2y+A==', 
                                        'timeCreated': '2019-04-02T23:09:30.066Z', 
                                        'time-updated-iso': '2019-04-02T23:09:30.066000+00:00', 
                                        'id': 'gbsc-gcp-project-mvp-dev-from-personalis-qc/dsub/vcfstats/concat/objects/concat_vcfstats.txt.csv/1554246570068077', 
                                        'contentType': 'text/csv', 
                                        'generation': '1554246570068077', 
                                        'metageneration': '1', 
                                        'kind': 'storage#object', 
                                        'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-qc/o/dsub%2Fvcfstats%2Fconcat%2Fobjects%2Fconcat_vcfstats.txt.csv?generation=1554246570068077&alt=media', 
                                        'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-qc/o/dsub%2Fvcfstats%2Fconcat%2Fobjects%2Fconcat_vcfstats.txt.csv', 
                                        'labels': ['Vcfstats', 'Concat', 'WGS_9000', 'Blob'], 
                                        'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis-qc', 
                                        'basename': 'concat_vcfstats.txt.csv', 
                                        'crc32c': 'wO1axA==', 
                                        'size': 3177403, 
                                        'timeStorageClassUpdated': '2019-04-02T23:09:30.066Z', 
                                        'task-group': 'vcfstats', 
                                        'name': 'concat_vcfstats', 
                                        'etag': 'CO2Qz9XDsuECEAE=', 
                                        'updated': '2019-04-02T23:09:30.066Z', 
                                        'time-created-epoch': 1554246570.066, 
                                        'time-updated-epoch': 1554246570.066
                               }
            }
    }
    data = json.dumps(data)
    data = data.encode('utf-8')
    
    event = {'data': base64.b64encode(data)}

    import_csv_to_bigquery(event, context=None)
