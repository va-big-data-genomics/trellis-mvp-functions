import os
import json
import base64

from google.cloud import storage

ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']

    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    REGIONS = parsed_vars['DSUB_REGIONS']
    #ZONES = parsed_vars['DSUB_ZONES']
    OUT_BUCKET = parsed_vars['DSUB_OUT_BUCKET']
    LOG_BUCKET = parsed_vars['DSUB_LOG_BUCKET']
    DSUB_USER = parsed_vars['DSUB_USER']
    TOPIC = parsed_vars['NEW_JOBS_TOPIC']

    PUBLISHER = pubsub.PublisherClient()

def format_pubsub_message(job_dict, nodes):
    message = {
               "header": {
                          "resource": "query", 
                          "method": "POST",
                          "labels": ["Job", "Delete", "Duplicates"],
                          "sentFrom": f"{FUNCTION_NAME}",
               },
               "body": {
                        "cypher": query, 
                        "result-mode": "stats"
               }
    }
    return message


def publish_to_topic(publisher, project_id, topic, data):
    topic_path = publisher.topic_path(project_id, topic)
    message = json.dumps(data).encode('utf-8')
    result = publisher.publish(topic_path, data=message).result()
    return result


def kill_duplicate_jobs(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    
    Update the metadata of a Blob specified by PubSub message.

    Args:
         event (dict): 'data' contains a string describing with the
                       bucket and name of a GCS blob in the format 
                       of : {bucket}#{name}
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    data = base64.b64decode(event['data']).decode('utf-8')
    event_id = context.event_id
    print(f"> Context: {context}.")
    print(f"> Data: {data}.")
    header = data['header']
    body = data['body']

    job = body['results']['job']
    duplicates = job.get('duplicateIds')
    if not duplicates:
        print("> No duplicates found. exiting.")

    for instance_id in duplicates:
        # Lookup each instance & delete
        