import os
import pdb
import sys
import json
import time
import uuid
import yaml
import base64
import logging

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


class TrellisMessage:

    def __init__(self, event, context):
        """Parse Trellis messages from Pub/Sub event & context.

        Args:
            event (type):
            context (type):

        Message format:
            - context
                - event_id (required)
            - event
                - header
                    - sentFrom (required)
                    - method (optional)
                    - resource (optional)
                    - labels (optional)
                    - seedId (optional)
                    - previousEventId (optional)
                - body
                    - cypher (optional)
                    - results (optional)
        """

        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)
        logging.info(f"> Context: {context}.")
        logging.info(f"> Data: {data}.")
        header = data['header']
        body = data['body']

        self.event_id = context.event_id
        self.seed_id = header.get('seedId')
        
        # If no seed specified, assume this is the seed event
        if not self.seed_id:
            self.seed_id = self.event_id

        self.header = data['header']
        self.body = data['body']

        self.results = {}
        if body.get('results'):
            self.results = body.get('results')

        self.node = None
        if self.results.get('node'):
            self.node = self.results['node']


def load_json(path):
    with open(path) as fh:
        data = json.load(fh)
    return data


def check_conditions(data_labels, node):
    required_labels = ['Blob']

    conditions = [
        # Check that all required labels are present
        set(required_labels).issubset(set(node.get('labels'))),
        # Check that one & only one type format class is represented
        len(set(data_labels).intersection(set(node.get('labels'))))==1,
    ]

    for condition in conditions:
        if condition:
            continue
        else:
            return False
    return True


def get_bigquery_config_data(tsv_configs, node):
    """Get BigQuery load configuration for node data type."""

    try:
        task_label = set(tsv_configs.keys()).intersection(set(node.get('labels'))).pop()
    except KeyError as exception:
        logging.error("No BigQuery configuration label found in node labels.")
        raise
    return tsv_configs[task_label]


def make_gcs_uri(node):
    bucket = node['bucket']
    path = node['path']
    uri = f"gs://{bucket}/{path}"
    return uri


def config_load_job():
    # Specify LoadJobConfig (https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.LoadJobConfig.html)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV 
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.field_delimiter = "\t"
    job_config.skip_leading_rows = 1
    job_config.null_marker = "NA"
    return job_config


def make_table_schema(schema_fields):
    # Create table schema (https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.schema.SchemaField.html)
    bq_schema = []
    for label, kind in schema_fields.items():
        bq_schema.append(bigquery.SchemaField(label, kind))
    return bq_schema


def append_tsv_to_bigquery(event, context):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """
    # Parse message
    message = TrellisMessage(event, context)

    # Check that message includes node metadata
    if not message.node:
        logging.warning("> No node provided. Exiting.")
        return(1)
    
    # Load BigQuery table config data
    bigquery_config = load_json('bigquery-config.json')
    tsv_configs = bigquery_config["TSV"]

    # Check whether node & message metadata meets function conditions
    conditions_met = check_conditions(
                                      data_labels = tsv_configs.keys(),
                                      node        = message.node)
    if not conditions_met:
        logging.error(f"> Input node does not match requirements. Node: {node}.")
        return(1)

    # Get BigQuery load configuration for node data type
    config_data = get_config_data(tsv_configs, message.node)

    # Get TSV URI
    tsv_uri = make_gcs_uri(message.node)
    
    # Configure BigQuery load job
    job_config = config_load_job()

    # Create table schema
    job_config.schema = make_table_schema(config_data['schema-fields'])
    logging.info(f"> Table schema: {job_config.schema}")

    # Print completed job configuration
    logging.info(f"> Job configuration: {job_config}.")

    # Append TSV to BigQuery table
    logging.info(f"> Loading table: {config_data['table-name']}.")
    logging.info(f"> {BIGQUERY_DATASET} {PROJECT_ID}.")
    dataset_ref = CLIENT.dataset(BIGQUERY_DATASET)
    try:
        load_job = CLIENT.load_table_from_uri(
                                    source_uris = tsv_uri, 
                                    destination = dataset_ref.table(name), 
                                    job_config = job_config)
    except exceptions.NotFound:
        # Check that table exists.
        # API seems to throw this exception even though it will
        #   automatically create the table if it doesn't exist.
        dataset_ref = CLIENT.dataset(BIGQUERY_DATASET)
        tables = list(CLIENT.list_tables(dataset_ref))
        print(f"Table created. Dataset tables: {tables}.")
    except:
        raise
    logging.info(f"Starting job {load_job.job_id}.")

    # QC: Check how many rows are in the table
    destination_table = CLIENT.get_table(dataset_ref.table(name))
    logging.info(f"Loaded {destination_table.num_rows} rows.")
    return 0

