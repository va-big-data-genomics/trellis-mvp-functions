import os
import re
import pdb
import json
import yaml
import base64
import logging
import importlib
import trellisdata as trellis

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
    DB_QUERY_TOPIC = parsed_vars.get('DB_QUERY_TOPIC')
    DATA_GROUP = parsed_vars.get('DATA_GROUP')
    DB_TRIGGERS = parsed_vars['DB_TRIGGERS']

    PUBLISHER = pubsub.PublisherClient()

    # Load trigger module
    #trigger_module_name = f"database-triggers"
    #triggers = importlib.import_module(trigger_module_name)
    #ALL_TRIGGERS = triggers.get_triggers(FUNCTION_NAME, parsed_vars)

    # Need to pull this from GCS
    trigger_document = storage.Client() \
                        .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                        .get_blob(os.environ[DB_TRIGGERS]) \
                        .download_as_string()
    #with open("database-triggers.yaml", 'r') as trigger_document:
    TRIGGER_CONTROLLER = trellis.TriggerController(trigger_document)

    #ALL_TRIGGERS = []
    #for config in triggers_config:
    #    config["function_name"] = FUNCTION_NAME
    #    config["env_vars"] = parsed_vars

    #    trigger = trellis.DatabaseTrigger(**config)
    #    ALL_TRIGGERS.append(trigger)

""" DEPRECATED in 1.3.0: use trellisdata.utils.publish_to_pubsub_topic
def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result
"""

def check_triggers(event, context, dry_run=False):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    query_response = trellis.QueryResponseReader(event, context)

    print(f"> Received message (context): {query_response.context}.")
    print(f"> Message header: {query_response.header}.")
    print(f"> Message body: {query_response.body}.")


    activated_triggers = TRIGGER_CONTROLLER.evaluate_trigger_conditions(query_response)
    for trigger, parameters in activated_triggers:
        logging.info(f"> Trigger activated: {trigger.name}.")

        # Create query request
        query_request = trellis.QueryRequestWriter(
            sender = FUNCTION_NAME,
            seed_id = query_response.seed_id,
            previous_event_id = query_response.event_id,
            query_name = trigger.query,
            query_parameters = parameters)
        
        pubsub_message = query_request.format_json_message()
        logging.info(f"> Publishing query request: {pubsub_message}.")
        if dry_run:
            logging.info(f"> Dry run: Would have published message to {DB_QUERY_TOPIC}.")
        else:
            result = trellis.publish_to_pubsub_topic(PUBLISHER, PROJECT_ID, DB_QUERY_TOPIC, pubsub_message)
            logging.info(f"> Published message to {DB_QUERY_TOPIC} with result: {result}.")
    return(activated_triggers)
