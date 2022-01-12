import os
import re
import pdb
import json
import yaml
import base64
import logging
import importlib

from google.cloud import storage
from google.cloud import pubsub

# Get runtime variables from cloud storage bucket
# https://www.sethvargo.com/secrets-in-serverless/
ENVIRONMENT = os.environ.get('ENVIRONMENT')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']

    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    # Runtime variables
    PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT')
    TOPIC = parsed_vars.get('DB_QUERY_TOPIC')

    PUBLISHER = pubsub.PublisherClient()

    # Load trigger module
    #trigger_module_name = f"database-triggers"
    #triggers = importlib.import_module(trigger_module_name)
    #ALL_TRIGGERS = triggers.get_triggers(FUNCTION_NAME, parsed_vars)
else:
    FUNCTION_NAME = "check-triggers"
    PROJECT_ID = "local"
    TOPIC = "db-dummy-topic"
    
    PUBLISHER = pubsub.PublisherClient()

# Import database triggers
import trellis_classes as trellis

# Load trigger configuration data
with open("database-triggers.json", 'r') as fh:
    triggers_config = json.load(fh)

# Create DatabaseTrigger objects
for config in triggers_config:
    trigger = trellis.DatabaseTrigger(config**)


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result


def check_triggers(event, context, dry_run=False):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    # Trellis config data
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    logging.info(f"> Received pubsub message: {data}.")
    header = data['header']
    body = data['body']

    resource = header['resource']
    results = body['results']

    # Check that resource is queryResult
    if not resource in ['queryResult', 'request']:
    #if resource != 'queryResult':
        raise ValueError(
                         f"Error: Expected resource type 'queryResult', " +
                         f"got '{header['resource']}.'")

    node = body['results'].get('node')


    activated_triggers = []
    for trigger in ALL_TRIGGERS:
        #status = trigger.check_conditions(node)
        logging.debug(f"> Checking trigger: {trigger}.")
        status = trigger.check_conditions(header, body, node)
        if status == True:
            activated_triggers.append(trigger)
            logging.info(f"> Trigger ACTIVATED: {trigger}.")
            #topic, message = trigger.compose_message(header, body, node)
            messages = trigger.compose_message(header, body, node, context)
            for message in messages:
                topic = message[0]
                data = message[1]
                logging.info(f"> Publishing message: {data}.")
                if dry_run:
                    logging.info(f"> Dry run: Would have published message to {topic}.")
                else:
                    result = publish_to_topic(topic, data)
                    logging.info(f"> Published message to {topic} with result: {result}.")
    return(activated_triggers)
