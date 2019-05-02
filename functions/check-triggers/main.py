import os
import re
import json
import yaml
import base64
import importlib

from google.cloud import storage
from google.cloud import pubsub

# Get runtime variables from cloud storage bucket
# https://www.sethvargo.com/secrets-in-serverless/
ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    TRIGGER = os.environ['TRIGGER']

    # Runtime variables
    PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT', '')
    TOPIC = parsed_vars.get('DB_QUERY_TOPIC', '')
    DATA_GROUP = parsed_vars.get('DATA_GROUP', '')

    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID,
                                                       topic = TOPIC)
def publish_message(topic_path, message):
    message = json.dumps(message).encode('utf-8')
    print(f'> Publishing message "{message}".')
    result = PUBLISHER.publish(topic_path, data=message).result()
    print(f'> Message published to {topic_path}: {result}.')   


def check_triggers(event, context):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    # Trellis config data
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"> Processing pubsub message: {data}.")
    result = json.loads(data['result'])

    trigger_module_name = f"{TRIGGER}_triggers"
    trigger_module = importlib.import_module(trigger_module_name)

    if TRIGGER == 'node':
        trigger_config = trigger_module.NodeTriggers(
                                                     project_id = PROJECT_ID,
                                                     node = result) #????
    elif TRIGGER == 'property':
        trigger_config = trigger_module.PropertyTriggers(
                                                         project_id = PROJECT_ID,
                                                         properties = result)

    triggers = trigger_config.get_triggers()
    #trigger_config.execute_triggers()
    for trigger in triggers:
        print(f'> Executing trigger: {triggers}.')
        topic_path, message = trigger()
        publish_message(topic_path, message)

    #summary = {
    #           "name": name, 
    #           "bucket": bucket_name, 
    #           "node-module-name": node_module_name, 
    #           "trigger-module-name": trigger_module_name, 
    #           "labels": labels, 
    #           "db-query": db_query,
    #}
    #return(summary)


if __name__ == "__main__":
    # Run unit tests in local
    PROJECT_ID = "***REMOVED***-dev"
    TOPIC = "wgs-35000-db-queries"
    DATA_GROUP = 'wgs-35000'
    TRIGGER = 'property'

    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID,
                                                       topic = TOPIC)

    data = {
             "resource": "query-result",
             "query": "<SOME QUERY>",
             "result": '{"added_setSize": 16, "nodes_sample": "SHIP4946367", "nodes_labels": ["Fastq", "WGS_35000", "Blob"]}' 

    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context)