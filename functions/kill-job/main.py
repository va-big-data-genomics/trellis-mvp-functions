import os
import json
import time
import base64
import logging

from google.cloud import storage
from googleapiclient import discovery
#from oauth2client.client import GoogleCredentials

#credentials = GoogleCredentials.get_application_default()
#SERVICE = discovery.build('compute', 'v1', credentials=credentials)
SERVICE = discovery.build('compute', 'v1')

ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    PROJECT_ID = os.environ['GCP_PROJECT']
    

def kill_job(event, context):
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

    job = body['results'].get('node')
    if not job:
        print("> No job found; exiting.")
        return  

    name = job['instanceName']
    zone = job['zone']

    # Send request to delete each duplicate job instance
    while True:
        try:
            logging.info(f"> Attempting to shutdown {zone}:{name}.")
            request = SERVICE.instances().delete(
                                                 project = PROJECT_ID,
                                                 zone = zone,
                                                 instance = name)
            response = request.execute()
            logging.info(f"> Response: {response}.")
            break
        except ConnectionResetError as error:
            logging.warn(f"> Encountered connection interruption: {error}.")
        