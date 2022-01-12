import os
import re
import pdb
import json
import yaml
import base64
import logging
import importlib

import trellis_classes as trellis

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
    DATA_GROUP = parsed_vars.get('DATA_GROUP')

    PUBLISHER = pubsub.PublisherClient()
else:
    parsed_vars = {}
    FUNCTION_NAME = "check-triggers"
    PROJECT_ID = "local"
    TOPIC = "db-dummy-topic"
    
    PUBLISHER = None

# Import database triggers
import trellis_classes as trellis

# Load trigger configuration data
with open("database-triggers.json", 'r') as fh:
    triggers_config = json.load(fh)

# Create DatabaseTrigger objects
ALL_TRIGGERS = []
for config in triggers_config:
    config["function_name"] = FUNCTION_NAME
    config["env_vars"] = parsed_vars

    trigger = trellis.DatabaseTrigger(**config)
    ALL_TRIGGERS.append(trigger)

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
    #message = trellis.TrellisMessage(event, context)
    message = trellis.TrellisMessage()
    message.parse_pubsub_message(event, context)
    logging.info(f"> Context: {message.context}.")
    logging.info(f"> Header: {message.header}.")
    logging.info(f"> Body: {message.body}.")

    # Check that resource is query
    supported_message_types = ['queryResult', 'request']
    if not message.header['resource'] in supported_message_types:
        raise ValueError(
                         f"Error: Expected resource of types {supported_message_types}, " +
                         f"got '{header['resource']}.'")

    activated_triggers = []
    for trigger in ALL_TRIGGERS:
        logging.debug(f"> Checking trigger: {trigger.name}.")
        status = trigger.check_all_conditions(message.header, message.body, message.node)
        if status == True:
            activated_triggers.append(trigger)
            logging.info(f"> Trigger ACTIVATED: {trigger.name}.")
            messages = trigger.format_json_messages(header, body, node, context)
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
