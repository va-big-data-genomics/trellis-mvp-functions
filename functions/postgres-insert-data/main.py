import os
import pdb
import sys
import json
import time
import uuid
import yaml
import base64
import logging
import psycopg2
#import sqlalchemy

from google.cloud import pubsub
from google.cloud import storage
from google.cloud import exceptions
#from google.cloud import error_reporting

ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']

    QC_DB_USER     = parsed_vars['QC_DB_USER']
    QC_DB_PASSWORD = parsed_vars['QC_DB_PASSWORD']
    QC_DB_NAME     = parsed_vars['QC_DB_NAME'] # postgres(?)
    #QC_DB_INSTANCE_CONN = parsed_vars['QC_DB_INSTANCE_CONN']
    # Required if only using private IP with VPC connector
    QC_DB_IP       = parsed_vars['QC_DB_IP']

    PUBLISHER = pubsub.PublisherClient()
    CLIENT = storage.Client()
    ERROR_CLIENT = error_reporting.Client()

    # Connect via psycopg2: https://stackoverflow.com/questions/52366380/how-to-connect-cloud-function-to-cloudsql
    # Google example: https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/cloud-sql/mysql/sqlalchemy/main.py
    DB_CONN = psycopg2.connect(
                               #host     = f'/cloudsql/{QC_DB_INSTANCE_CONN}',
                               host = QC_DB_IP,
                               dbname  = QC_DB_NAME,
                               user     = QC_DB_USER,
                               password = QC_DB_PASSWORD)

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


def format_pubsub_message(job_dict, seed_id, event_id):
    message = {
        "header": {
            "resource": "job-metadata",
            "method": "POST",
            "labels": ["Create", "Job", "PostgresInsertData", "Node"],
            "sentFrom": f"{FUNCTION_NAME}",
            "seedId": f"{seed_id}",
            "previousEventId": f"{event_id}"
        },
        "body": {
            "node": job_dict,
        }
    }
    return message


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


def get_table_config_data(table_configs, node):
    """Get BigQuery load configuration for node data type."""

    try:
        task_label = set(table_configs.keys()).intersection(set(node.get('labels'))).pop()
    except KeyError as exception:
        logging.error("No table configuration label found in node labels.")
        raise
    return table_configs[task_label]


def table_exists(connection, table_name):
    """

    tested locally: true
    From: https://stackoverflow.com/questions/10598002/how-do-i-get-tables-in-postgres-using-psycopg2
    """
    exists = False
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT EXISTS(SELECT relname FROM pg_class WHERE relname='{table_name}')")
        exists = cursor.fetchone()[0]
        #print(exists)
        cursor.close()
    except psycopg2.Error as e:
        logging.error(e)
    return exists


def get_table_col_names(connection, table_str):
    """

    tested locally: true
    From: https://stackoverflow.com/questions/10598002/how-do-i-get-tables-in-postgres-using-psycopg2
    """
    col_names = []
    try:
        cursor = connection.cursor()
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        for desc in cursor.description:
            col_names.append(desc[0])        
        cursor.close()
    except psycopg2.Error as e:
        logging.error(e)
    return col_names


def create_table_sql(table_name, schema_fields):
    """

    tested locally: true
    From: https://www.postgresqltutorial.com/postgresql-python/create-tables/
    """

    schema_strings = []
    for column, data_type in schema_fields.items():
        schema_strings.append(f"{column} {data_type}")
    schema_str = ",".join(schema_strings)

    sql = f"CREATE TABLE {table_name} ({schema_str})"
    return sql


def execute_sql_command(connection, command):
    try:
        cursor = connection.cursor()
        # Run SQL command
        cursor.execute(command)
        # close communication with the PostgreSQL database server
        cursor.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def insert_multiple_rows(conn, table_name, schema_fields, rows):
    """ Insert multiple rows into table.

    tested locally: true
    Adapted from: https://www.postgresqltutorial.com/postgresql-python/insert/
    """
    columns = ','.join(schema_fields.keys())
    value_symbols = []
    for column in schema_fields.keys():
        value_symbols.append('%s')
    values_string = ', '.join(value_symbols)
    sql = f"INSERT INTO {table_name}({columns}) VALUES({values_string})"
    print(sql)
    try:
        cursor = conn.cursor()
        cursor.executemany(sql, rows)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def postgres_insert_data(event, context):
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

    extension = message.node['extension'].upper()
    
    # Load table config data
    table_config = load_json('postgres-config.json')
    extension_configs = table_config[extension]

    # Check whether node & message metadata meets function conditions
    conditions_met = check_conditions(
                                      data_labels = extension_configs.keys(),
                                      node        = message.node)
    if not conditions_met:
        logging.error(f"> Input node does not match requirements. Node: {node}.")
        return(1)

    # Get table configuration for node data type
    # TestGetTableConfigData
    config_data = get_table_config_data(extension_configs, message.node)
    table_name = config_data['table-name']
    schema_fields = config_data['schema-fields']
    # https://cloud.google.com/functions/docs/monitoring/error-reporting
    logging.info(RuntimeError(f"Table name: {table_name}."))
    logging.info(RuntimeError(f"Schema fields: {schema_fields}."))

    # TODO: this is broken
    # Check whether table exists
    table_exists = table_exists(DB_CONN, table_name)

    # Debugging
    return

    if not table_exists:
        # If not, create table
        sql = create_table_sql(table_name, schema_fields)
        execute_sql_command(DB_CONN, sql)

    # Check that table columns match listed schema
    col_names = get_table_col_names(DB_CONN, table_name)
    if not col_names == schema_fields.keys():
        logging.error("Column names do not match schema.")

    # Get CSV data
    bucket_name = node['bucket']
    bucket = CLIENT.get_bucket(bucket_name)
    blob = bucket.get_blob(node['path'])
    data = blob.download_as_string().decode('utf-8')

    if node['extension'] == 'tsv':
        delimiter = "\t"
    elif node['extension'] == 'csv':
        delimiter = ","
    else:
        logging.error("Extension \"{node['extension']}\" does not match supported types.")
        return None

    # Separate string into lines
    lines = data.split('\n')

    # Separate line into columns
    rows = []
    for line in lines:
        columns = line.split(delimiter)
        row = tuple(columns)
        rows.append(row)

    # Insert rows into table
    insert_multiple_rows(DB_CONN, table_name, schema_fields, rows)

    job_dict = {
                "databaseName": QC_DB_NAME,
                "tableName": table_name,
                "inputIds": [message.node['id']],
                "labels": [
                           'Job',
                           'PostgresInsertData']
    }

    # Publish job node information
    message = format_pubsub_message(
                                    job_dict,
                                    message.seed_id,
                                    message.event_id)
    logging.info(f"> Pubsub message: {message}.")
    result = publish_to_topic(NEW_JOBS_TOPIC, message)
    logging.info(f"> Published message to {NEW_JOBS_TOPIC} with result: {result}.")


