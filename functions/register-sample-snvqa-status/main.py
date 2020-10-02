import os
import re
import pdb
import json
import yaml

from datetime import datetime

from google.cloud import storage
from google.cloud import pubsub

# Get runtime variables from cloud storage bucket
# https://www.sethvargo.com/secrets-in-serverless/
ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    TRIGGER_OPERATION = os.environ['TRIGGER_OPERATION']
    GIT_COMMIT_HASH = os.environ['GIT_COMMIT_HASH']
    GIT_VERSION_TAG = os.environ['GIT_VERSION_TAG']

    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    # Runtime variables
    PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT')
    DB_QUERY_TOPIC = parsed_vars.get('DB_QUERY_TOPIC')
    TOPIC_TRIGGERS = parsed_vars.get('TOPIC_TRIGGERS')

    PUBLISHER = pubsub.PublisherClient()
    STORAGE = storage.Client()


def format_pubsub_message(query, seed_id):
    message = {
               "header": {
                          "resource": "query",
                          "method": "POST",
                          "labels": ["Update", "Sample", "Node", "Cypher", "Query"],
                          "sentFrom": f"{FUNCTION_NAME}",
                          "publishTo": f"{TOPIC_TRIGGERS}",
                          "seedId": f"{seed_id}",
                          "previousEventId": f"{seed_id}"
               },
               "body": {
                        "cypher": query,
                        "result-mode": "data",
                        "result-structure": "list",
                        "result-split": "True",
               },
    }
    return message


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result


def register_sample_snvqa_status(event, context):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    print(f"> Processing new object event: {event['name']}.")
    print(f"> Event: {event}).")
    print(f"> Context: {context}.")

    seed_id = context.event_id

    object_bucket = event['bucket']
    object_path = event['name']

    # Check that object matches expected input path
    object_pattern = "analysis-notebooks/sample-status.csv"
    if not object_path == object_pattern:
        logging.warning("Object path does not match specified pattern. Ignoring.")
        return

    # Load CSV object (format: "sample,status[pass,fail]")
    bucket = STORAGE.get_bucket(object_bucket)
    blob = bucket.get_blob(object_path)
    csv_data = blob.download_as_string()

    for line in csv_data:
        elements = line.split(',')
        sample = elements[0]
        status = elements[1]
        neo4j_status = None

        if status == 'pass':
            neo4j_status = True
        elif status == 'fail':
            neo4j_status = False

        # Create a query to update :Sample node
        db_query = _create_query(sample, neo4j_status)

        message = format_pubsub_message(db_query, seed_id)
        print(f"> Pubsub message: {message}.")
        result = publish_to_topic(DB_QUERY_TOPIC, message)
        print(f"> Published message to {DB_QUERY_TOPIC} with result: {result}.")


def _create_query(sample, neo4j_status):

    query = (
        "MATCH (node:Sample) " +
        f"WHERE node.sample = {sample} " +
        f"SET node.trellis_snvQa = {neo4j_status} " +
        "RETURN node")
    return query

