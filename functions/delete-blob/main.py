import os
import json
import base64
import logging

from google.cloud import storage

CLIENT = storage.Client()

ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    PROJECT_ID = os.environ['GCP_PROJECT']
    

def delete_blob(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    
    Update the metadata of a Blob specified by PubSub message.

    Args:
         event (dict): 'data' contains a string describing with the
                       bucket and name of a GCS blob in the format 
                       of : {bucket}#{name}
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    event_id = context.event_id
    data = json.loads(pubsub_message)
    print(f"> Context: {context}.")
    print(f"> Data: {data}.")

    header = data['header']
    body = data['body']

    blob = body['results'].get('node')
    if not blob:
        print("> No blob found; exiting.")
        return  

    bucket = blob['bucket']
    path = blob['path']

    logging.info(f"> Attempting to delete blob gs://{bucket}/{path}.")
    
    bucket = CLIENT.get_bucket(bucket)
    blob = bucket.blob(path)
    blob.delete()
    
    logging.info(f"> Blob gs://{bucket}/{path} deleted.")
        