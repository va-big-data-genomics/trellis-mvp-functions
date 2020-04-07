import os
import re
import pdb
import sys
import json
import pytz
import yaml
import base64
import iso8601

from datetime import datetime

from google.cloud import storage
from google.cloud import pubsub

# Get runtime variables from cloud storage bucket
# https://www.sethvargo.com/secrets-in-serverless/
ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']

    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    # Runtime variables
    PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT')
    DB_TOPIC = parsed_vars.get('DB_QUERY_TOPIC')
    TOPIC_TRIGGERS = parsed_vars.get('TOPIC_TRIGGERS')
    DATA_GROUP = parsed_vars.get('DATA_GROUP')

    PUBLISHER = pubsub.PublisherClient()


def format_pubsub_message(query, event_id, publish_to=None):
    message = {
               "header": {
                          "resource": "query",
                          "method": "UPDATE",
                          "labels": ['Update', 'Job', 'Node', 'Query', 'Cypher'],
                          "sentFrom": f"{FUNCTION_NAME}",
                          "seedId": f"{event_id}",
                          "previousEventId": f"{event_id}"
               },
               "body": {
                        "cypher": query,
                        "result-mode": "data",
                        "result-structure": "list",
                        "result-split": "True",
                },
    }
    if publish_to:
        extension = {"publishTo": publish_to}
        message['header'].update(extension)
    return message


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result


def get_seconds_from_epoch(datetime_obj):
    """Get datetime as total seconds from epoch.

    Provides datetime in easily sortable format

    Args:
        datetime_obj (datetime): Datetime.
    Returns:
        (float): Seconds from epoch
    """
    from_epoch = datetime_obj - datetime(1970, 1, 1, tzinfo=pytz.UTC)
    from_epoch_seconds = from_epoch.total_seconds()
    return from_epoch_seconds


def get_datetime_iso8601(date_string):
    """ Convert ISO 86801 date strings to datetime objects.

    Google datetime format: https://tools.ietf.org/html/rfc3339
    ISO 8601 standard format: https://en.wikipedia.org/wiki/ISO_8601

    Args:
        date_string (str): Date in ISO 8601 format
    Returns
        (datetime.datetime): Datetime objects
    """
    return iso8601.parse_date(date_string)


def log_delete_instance(event, context):
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

    status = "STOPPED"

    # Get seed/event ID to track provenance of Trellis events
    event_id = context.event_id

    payload = data['protoPayload']
    resource = data['resource']

    # resourceName provides entire project path
    name_path = payload['resourceName']
    instance_name = name_path.split('/')[-1]

    instance_id = resource['labels']['instance_id']

    stop_time = data['timestamp']
    timestamp = get_datetime_iso8601(data['timestamp'])
    stop_time_epoch = get_seconds_from_epoch(timestamp)

    # Find out which agent stopped the VM
    auth_email = payload['authenticationInfo']['principalEmail']
    domain = auth_email.split('@')[1]
    service = domain.split('.')[0]
    stopped_by = service

    query = (
            f"MATCH (node:Job {{ " +
                f"instanceId: {instance_id}, " +
                f"instanceName: \"{instance_name}\" }} ) " +
             "SET " +
                f"node.stopTime = \"{stop_time}\", " +
                f"node.stopTimeEpoch = {stop_time_epoch}, " +
                f"node.stoppedBy = \"{stopped_by}\", " +
                f"node.status = \"{status}\", " +
                "node.durationMinutes = " +
                    "duration.inSeconds(datetime(node.startTime), " +
                    "datetime(node.stopTime)).minutes " +
             "RETURN node")

    # If an instance cannot be found (i.e. already deleted),
    # delete operation will not return an instance ID.
    # For now, I'm just ignoring these messages.
    if not instance_id:
        print(f"> No instance ID provided; skipping.")
        return

    print(f"> Database query: \"{query}\".")

    message = format_pubsub_message(
                                    query = query,
                                    event_id = event_id,
                                    publish_to = TOPIC_TRIGGERS)
    print(f"> Pubsub message: {message}.")

    result = publish_to_topic(DB_TOPIC, message)
    print(f"> Published message to {DB_TOPIC} with result: {result}.")


if __name__ == "__main__":
    # Run unit tests in local
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"
    DB_TOPIC = "wgs35-db-queries"
    KILL_DUPS_TOPIC = "wgs35-kill-duplicate-jobs"
    DATA_GROUP = "wgs35"
    FUNCTION_NAME = "wgs35-update-job-status"

    PUBLISHER = pubsub.PublisherClient()

    # Insert operation
    with open("insert_data_sample.json", "r") as fh:
        data = json.load(fh)
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = {'test': 123}
    update_job_status(event, context)

    pdb.set_trace()

    # Delete operation
    with open("delete_data_sample.json", "r") as fh:
        data = json.load(fh)
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = {'event_id': 611225247182937, 'timestamp': '2019-07-10T04:11:53.865Z', 'event_type': 'google.pubsub.topic.publish', 'resource': {'service': 'pubsub.googleapis.com', 'name': 'projects/gbsc-gcp-project-mvp-test/topics/wgs35-update-vm-status', 'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage'}}

    update_job_status(event, context)
