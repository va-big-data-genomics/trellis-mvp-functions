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
    # set up the Google Cloud Logging python client library
    # source: https://cloud.google.com/blog/products/devops-sre/google-cloud-logging-python-client-library-v3-0-0-release
    import google.cloud.logging
    client = google.cloud.logging.Client()
    client.setup_logging()

    # use Python's standard logging library to send logs to GCP
    import logging

    FUNCTION_NAME = os.environ['FUNCTION_NAME']

    config_doc = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    # https://stackoverflow.com/questions/6866600/how-to-parse-read-a-yaml-file-into-a-python-object
    TRELLIS = yaml.safe_load(config_doc)

    PUBLISHER = pubsub.PublisherClient()

    # Need to pull this from GCS
    trigger_document = storage.Client() \
                        .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                        .get_blob(TRELLIS['DB_TRIGGERS']) \
                        .download_as_string()
    TRIGGER_CONTROLLER = trellis.TriggerController(trigger_document)

def check_triggers(event, context, dry_run=False):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    query_response = trellis.QueryResponseReader(
                        context = context,
                        event = event)

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
            logging.info(f"> Dry run: Would have published message to {TRELLIS['TOPIC_DB_QUERY']}.")
        else:
            result = trellis.utils.publish_to_pubsub_topic(
                                                           PUBLISHER, 
                                                           TRELLIS['PROJECT_ID'], 
                                                           TRELLIS['TOPIC_DB_QUERY'], 
                                                           pubsub_message)
            logging.info(f"> Published message to {TRELLIS['TOPIC_DB_QUERY']} with result: {result}.")
    return(activated_triggers)
