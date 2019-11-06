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


def format_pubsub_message(query, publish_to=None):
    message = {
               "header": {
                          "resource": "query",
                          "method": "Create",
                          "labels": ['Create', 'Job', 'CromwellAttempt', 'Node', 'Query', 'Cypher'], 
                          "sentFrom": f"{FUNCTION_NAME}",
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


def log_insert_cromwell_instance(event, context):
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

    status = "RUNNING"

    cromwell_fields = {}

    payload = data['protoPayload']
    resource = data['resource']

    # Get task ID from request
    # labels: [{0: {key: "trellis-id", value: "1907-5fxf7"}}]
    labels = payload['request']['labels']
    for label in labels:
        key = label['key']
        value = label['value']
        
        # Get Cromwell specific metadata values
        if key == 'cromwell-workflow-id':
            cromwell_fields['cromwellWorkflowId'] = value
        elif key == 'goog-pipelines-worker':
            cromwell_fields['googPipelinesWorker'] = value
        elif key == 'wdl-call-alias':
            cromwell_fields['wdlCallAlias'] = value
        elif key == 'wdl-task-name':
            cromwell_fields['wdlTaskName'] = value

    instance_name = payload['request']['name']

    machine_type = payload['request']['machineType']
    machine_type = machine_type.split('/')[-1]

    instance_id = resource['labels']['instance_id']
    zone = resource['labels']['zone']
    project = resource['labels']['project_id']

    start_time = data['timestamp']

    timestamp = get_datetime_iso8601(data['timestamp'])
    start_time_epoch = get_seconds_from_epoch(timestamp)

    cromwell_strings = []
    for key, value in cromwell_fields.items():
        query_str = f"node.{key} = \"{value}\""
        cromwell_strings.append(query_str)
    if cromwell_strings:
        cromwell_query_str = ", ".join(cromwell_strings)
    else:
        cromwell_query_str = ''

    query = (
        "MERGE (node:Job {instanceName:" + f"\"{instance_name}\"" + "}) " +
        "ON CREATE SET " +
             "node.labels = [\"Job\", \"CromwellAttempt\", \"GcpInstance\"] " +
             "node:CromwellAttempt:GcpInstance "
        "SET " +
            f"node.status = \"{status}\", " +
            f"node.instanceName = \"{instance_name}\", " +
            f"node.instanceId = {instance_id}, " +
            f"node.startTime = \"{start_time}\", " +
            f"node.startTimeEpoch = {start_time_epoch}, " +
            f"node.zone = \"{zone}\", " +
            f"node.machineType = \"{machine_type}\", " +
            # Add Cromwell metadata fields, if present
            f"{cromwell_query_str} " +
        "RETURN node")

    # If an instance cannot be found (i.e. already deleted), 
    # delete operation will not return an instance ID.
    # For now, I'm just ignoring these messages.
    if not instance_id:
        print(f"> No instance ID provided; skipping.")
        return

    print(f"> Database query: \"{query}\".")

    message = format_pubsub_message(query, publish_to=TOPIC_TRIGGERS)
    print(f"> Pubsub message: {message}.")

    result = publish_to_topic(DB_TOPIC, message)
    print(f"> Published message to {DB_TOPIC} with result: {result}.")
    

if __name__ == "__main__":
    # Run unit tests in local
    PROJECT_ID = "***REMOVED***-dev"
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
    context = {'event_id': 611225247182937, 'timestamp': '2019-07-10T04:11:53.865Z', 'event_type': 'google.pubsub.topic.publish', 'resource': {'service': 'pubsub.googleapis.com', 'name': 'projects/***REMOVED***-test/topics/wgs35-update-vm-status', 'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage'}}

    update_job_status(event, context)

