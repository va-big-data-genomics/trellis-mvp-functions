import os
import pdb
import sys
import json
import time
import uuid
import yaml
import base64

#from google.cloud import pubsub
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import exceptions

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
    #BIGQUERY_CONFIG = parsed_vars['BIGQUERY_CONFIG']

    #PUBLISHER = pubsub.PublisherClient()
    CLIENT = bigquery.Client()


def append_tsv(name, csv_uri, schema, project, dataset):
    print(f"Loading table: {self.name}.")
    print(f"{BIGQUERY_DATASET} {PROJECT_ID}.")
    
    dataset_ref = CLIENT.dataset(BIGQUERY_DATASET)
    
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV 
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.field_delimiter = "\t"
    job_config.skip_leading_rows = 1
    job_config.null_marker = "NA"

    bq_schema = []
    for label, kind in schema_fields.items():
        bq_schema.append(bigquery.SchemaField(label, kind))
    job_config.schema = bq_schema
    print(f"Table schema: {job_config.schema}")

    print(f"Job configuration: {job_config}.")
    load_job = CLIENT.load_table_from_uri(
                                source_uris = csv_uri, 
                                destination = dataset_ref.table(name), 
                                job_config = job_config)
    print(f"Starting job {load_job.job_id}.")

    destination_table = CLIENT.get_table(dataset_ref.table(name))
    print(f"Loaded {destination_table.num_rows} rows.")


def append_tsv_to_bigquery(event, context):
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

    # Get BigQuery table configurations from cloud storage
    #config_blob = storage.Client() \
    #    .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
    #    .get_blob(os.environ['BIGQUERY_CONFIG']) \
    #    .download_as_string()
    #bigquery_configs = json.load(config_blob)
    
    with open('bigquery-config.json') as fh:
        bigquery_configs = json.load(fh)
    tsv_configs = bigquery_configs["TSV"]

    required_labels = ['Blob']
    conditions = [
        # Check that all required labels are present
        set(required_labels).issubset(set(node.get('labels'))),
        # Check that one & only one type format class is represented
        len(set(tsv_configs.keys()).intersection(set(node.get('labels'))))==1,
    ]

    for condition in conditions:
        if condition:
            continue
        else:
            logging.error(f"Input node does not match requirements. Node: {node}.")
            return False

    task_label = set(tsv_configs.keys()).intersection(set(node.get('labels'))).pop()
    config_data = tsv_configs[task_label]

    tsv_bucket = node['bucket']
    tsv_path = node['path']
    tsv_uri = f"gs://{tsv_bucket}/{tsv_path}"

    try:
        append_tsv(
                   name = config_data['tabel-name'],
                   tsv_uri = tsv_uri,
                   schema = config_data['schema-fields'],
                   project = PROJECT_ID,
                   dataset = BIGQUERY_DATASET)
    except exceptions.NotFound:
        # NOTE: This isn't actually doing anything.
        #print(f"Table not found. Creating table.")
        dataset_ref = CLIENT.dataset(BIGQUERY_DATASET)
        #CLIENT.create_table(bq_table)
        tables = list(CLIENT.list_tables(dataset_ref))
        print(f"Table created. Dataset tables: {tables}.")
    except:
        raise

# For local testing
if __name__ == "__main__":
    """
    PROJECT_ID = "***REMOVED***-dev"
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
                                        'id': '***REMOVED***-dev-from-personalis-qc/dsub/vcfstats/concat/objects/concat_vcfstats.txt.csv/1554246570068077', 
                                        'contentType': 'text/csv', 
                                        'generation': '1554246570068077', 
                                        'metageneration': '1', 
                                        'kind': 'storage#object', 
                                        'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-qc/o/dsub%2Fvcfstats%2Fconcat%2Fobjects%2Fconcat_vcfstats.txt.csv?generation=1554246570068077&alt=media', 
                                        'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-qc/o/dsub%2Fvcfstats%2Fconcat%2Fobjects%2Fconcat_vcfstats.txt.csv', 
                                        'labels': ['Vcfstats', 'Concat', 'WGS_9000', 'Blob'], 
                                        'bucket': '***REMOVED***-dev-from-personalis-qc', 
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
    """
