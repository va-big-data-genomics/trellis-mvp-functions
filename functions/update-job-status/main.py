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
    KILL_TOPIC = parsed_vars.get('KILL_VM_TOPIC')
    DATA_GROUP = parsed_vars.get('DATA_GROUP')

    PUBLISHER = pubsub.PublisherClient()


class InsertOperation:

    def __init__(self, data):

        self.instance_name = None
        self.instance_id = None
        self.machine_type = None
        self.start_time = None
        self.start_time_epoch = None
        self.task_id = None
        self.status = "running"

        payload = data['protoPayload']
        resource = data['resource']

        # Get task ID from request labels
        # labels: [{0: {key: "trellis-id", value: "1907-5fxf7"}}]
        labels = payload['request']['labels']
        for label in labels:
            if label['key'] == 'trellis-id':
                self.task_id = label['value']

        self.instance_name = payload['request']['name']

        machine_type = payload['request']['machineType']
        self.machine_type = machine_type.split('/')[-1]

        self.instance_id = resource['labels']['instance_id']

        self.start_time = data['timestamp']

        timestamp = get_datetime_iso8601(data['timestamp'])
        self.start_time_epoch = get_seconds_from_epoch(timestamp)

        
class DeleteOperation:

    def __init__(self, data):

        self.instance_name = None
        self.instance_id = None
        self.stop_time = None
        self.stop_time_epoch = None
        self.status = "stopped"

        payload = data['jsonPayload']

        self.instance_name = payload['resource']['name']
        self.instance_id = payload['resource']['id']

        self.stop_time = data['timestamp']
        timestamp = get_datetime_iso8601(data['timestamp'])
        self.stop_time_epoch = get_seconds_from_epoch(timestamp)


def format_pubsub_message(query, publish_to=None, perpetuate=None):
    message = {
               "header": {
                          "resource": "query",
                          "method": "POST", 
                          "labels": ['Job', 'Create', 'Node', 'Query', 'Cypher'], 
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
    if perpetuate:
        extension = {"perpetuate": perpetuate}
        message['body'].update(extension)
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


def compose_insert_query(insert_op):
    query = (
             "MERGE (node:Job {taskId: " + f"\"{insert_op.task_id}\"" + "}) " + 
             "ON CREATE SET " +
            f"node.status = \"{insert_op.status}\", " +
            f"node.instanceName = \"{insert_op.instance_name}\", " +
            f"node.instanceId = {insert_op.instance_id}, " +
            f"node.instanceStarted = \"{insert_op.start_time}\", " +
            f"node.instanceStartedEpoch = {insert_op.start_time_epoch}, " +
            f"node.duplicateIds = [] " +
             "ON MATCH SET " +
            f"node.duplicateIds = node.duplicateIds + {insert_op.instance_id} " +
             "RETURN node")
    return query


def compose_delete_query(delete_op):
    query = (
             "MATCH (job:Job) " +
            f"WHERE job.instanceId={delete_op.instance_id} " +
            f"AND job.instanceName=\"{delete_op.instance_name}\" " +
            f"SET job.stopTime=\"{delete_op.stop_time}\", " +
            f"job.stopTimeEpoch={delete_op.stop_time_epoch} " +
             "RETURN job")
    return query


def update_job_status(event, context):
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

    # Handle insert cases
    insert_method = 'v1.compute.instances.insert'
    delete_event = 'compute.instances.delete'
    
    insert = False
    if data.get('protoPayload'):
        if data.get('protoPayload').get('methodName') == insert_method:
            insert_op = InsertOperation(data)
            query = compose_insert_query(insert_op)
            insert = True
    elif data.get('jsonPayload'):
        if data.get('jsonPayload').get('event_subtype') == delete_event:
            delete_op = DeleteOperation(data)
            query = compose_delete_query(delete_op)
    print(f"> Database query: \"{query}\".")

    if insert:
        message = format_pubsub_message(query, publish_to=KILL_TOPIC)
    else:
        message = format_pubsub_message(query)
    print(f"> Pubsub message: {message}.")

    result = publish_to_topic(DB_TOPIC, message)
    print(f"> Published message to {DB_TOPIC} with result: {result}.")
    

if __name__ == "__main__":
    # Run unit tests in local
    PROJECT_ID = "***REMOVED***-dev"
    DB_TOPIC = "wgs35-db-queries"
    KILL_TOPIC = "wgs35-delete-instance"
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

    #pdb.set_trace()
    sys.exit()

    # Delete operation
    data = {"insertId":"8vo5t4g1d17tob","jsonPayload":{"actor":{"user":"service-133012913968@genomics-api.google.com.iam.gserviceaccount.com"},"event_subtype":"compute.instances.delete","event_timestamp_us":"1562731913309726","event_type":"GCE_OPERATION_DONE","operation":{"id":"6066364432740047756","name":"operation-1562731875193-58d4bde4cd364-1f29351a-caac1bca","type":"operation","zone":"us-west1-b"},"resource":{"id":"5162806143768987474","name":"google-pipelines-worker-b094108ab1ceec43210edcc78e27b50a","type":"instance","zone":"us-west1-b"},"trace_id":"operation-1562731875193-58d4bde4cd364-1f29351a-caac1bca","version":"1.2"},"labels":{"compute.googleapis.com/resource_id":"5162806143768987474","compute.googleapis.com/resource_name":"google-pipelines-worker-b094108ab1ceec43210edcc78e27b50a","compute.googleapis.com/resource_type":"instance","compute.googleapis.com/resource_zone":"us-west1-b"},"logName":"projects/***REMOVED***-test/logs/compute.googleapis.com%2Factivity_log","receiveTimestamp":"2019-07-10T04:11:53.365559788Z","resource":{"labels":{"instance_id":"5162806143768987474","project_id":"***REMOVED***-test","zone":"us-west1-b"},"type":"gce_instance"},"severity":"INFO","timestamp":"2019-07-10T04:11:53.309726Z"}
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}   
    context = {'event_id': 611225247182937, 'timestamp': '2019-07-10T04:11:53.865Z', 'event_type': 'google.pubsub.topic.publish', 'resource': {'service': 'pubsub.googleapis.com', 'name': 'projects/***REMOVED***-test/topics/wgs35-update-vm-status', 'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage'}}

    update_job_status(event, context)

