import os
import re
import pdb
import json
import pytz
import yaml
import base64
import iso8601
import importlib

from datetime import datetime

from google.cloud import storage
from google.cloud import pubsub

import trellisdata as trellis

# Get runtime variables from cloud storage bucket
# https://www.sethvargo.com/secrets-in-serverless/
ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':

    # set up the Google Cloud Logging python client library
    # source: https://cloud.google.com/blog/products/devops-sre/google-cloud-logging-python-client-library-v3-0-0-release
    import google.cloud.logging
    client = google.cloud.logging.Client()
    # log_level=10 is equivalent to DEBUG; default is 20 == INFO
    # NOTE: this debug setting doesn't work
    # Gcloud Python logging client: https://googleapis.dev/python/logging/latest/client.html?highlight=setup_logging#google.cloud.logging_v2.client.Client.setup_logging
    # Logging levels: https://docs.python.org/3/library/logging.html#logging-levels
    client.setup_logging(log_level=10)
    import logging
    
    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    GIT_COMMIT_HASH = os.environ['GIT_COMMIT_HASH']
    GIT_VERSION_TAG = os.environ['GIT_VERSION_TAG']

    config_doc = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    TRELLIS = yaml.safe_load(config_doc)

    # Runtime variables
    #PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT')
    #TOPIC = parsed_vars.get('DB_QUERY_TOPIC')
    #DATA_GROUP = parsed_vars.get('DATA_GROUP')
    #TOPIC_TRIGGERS = parsed_vars.get('TOPIC_TRIGGERS')

    PUBLISHER = pubsub.PublisherClient()

"""Deprecated with trellisdata message classes
def format_pubsub_message(query, seed_id, event_id):
    message = {
               "header": {
                          "resource": "query",
                          "method": "POST",
                          "labels": ['Create', 'Job', 'Node', 'Query', 'Cypher'],
                          "sentFrom": f"{FUNCTION_NAME}",
                          "publishTo": f"{TOPIC_TRIGGERS}",
                          "seedId": f"{seed_id}",
                          "previousEventId": f"{event_id}"
               },
               "body": {
                        "cypher": query,
                        "result-mode": "data",
                        "result-structure": "list",
                        "result-split": "True",
                },
    }
    return message
"""

"""
def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result
"""

def clean_metadata_dict(raw_dict):
    """Remove dict entries where the value is of type dict"""
    clean_dict = dict(raw_dict)

    # Remove values that are dicts
    delete_keys = []
    for key, value in clean_dict.items():
        if isinstance(value, dict):
            #del clean_dict[key]
            delete_keys.append(key)

    for key in delete_keys:
        del clean_dict[key]

    # Convert size field from str to int
    if clean_dict.get('size'):
        clead_dict['size'] = int(clean_dict['size'])

    return clean_dict


def format_query(db_entry, dry_run=False):
    labels = list(db_entry['labels'])
    labels.remove('Job')
    labels_str = ':'.join(labels)

    # Create database entry string
    entry_strings = []
    for key, value in db_entry.items():
        if isinstance(value, str):
            entry_strings.append(f'node.{key}="{value}"')
        else:
            entry_strings.append(f'node.{key}={value}')
    entry_string = ', '.join(entry_strings)

    # Format as cypher query
    #query = (
    #         f"CREATE (node:{labels_str} " +
    #         f"{{ {entry_string}, nodeCreated: timestamp() }}) " +
    #          "RETURN node")

    # TODO: Merge on :Job label to be
    query = (
             "MERGE (node:Job { " +
                f"trellisTaskId:\"{db_entry['trellisTaskId']}\" " +
             "}) " +
             "ON CREATE SET " +
                f"node :{labels_str}, " +
                f"{entry_string}, " +
                "node.nodeCreated= timestamp() " +
             "ON MATCH SET " +
                f"node :{labels_str}, " +
                f"{entry_string} " +
             "RETURN node")
    return query


def publish_create_job_node_query(event, context):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    #print(f"> Processing new Pub/Sub message: {context['event_id']}.")
    print(f"> Context: {context}.")
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"> Data: {data}.")

    header = data['header']
    body = data['body']

    event_id = context.event_id
    seed_id = header['seedId']

    # Create dict of metadata to add to database node
    db_dict = clean_metadata_dict(body['node'])

    # Add git version info
    db_dict['gitCommitHash'] = GIT_COMMIT_HASH
    db_dict['gitVersionTag'] = GIT_VERSION_TAG

    # Add standard fields
    time_fields = get_standard_time_fields(event)
    db_dict.update(time_fields)

    print(f"> Generating database query for node: {db_dict}.")
    db_query = format_query(db_dict)
    print(f"> Database query: \"{db_query}\".")

    message = format_pubsub_message(
                                    query = db_query,
                                    seed_id = seed_id,
                                    event_id = event_id)
    print(f"> Pubsub message: {message}.")
    result = publish_to_topic(TOPIC, message)
    print(f"> Published message to {TOPIC} with result: {result}.")
