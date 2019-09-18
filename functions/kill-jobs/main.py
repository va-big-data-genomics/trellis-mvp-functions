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


def delete_instance(zone, name):
    """DEPRECATED"""
    try:
        request = SERVICE.instances().delete(
                                             project = PROJECT_ID,
                                             zone = zone,
                                             instance = name)
        response = request.execute()
        return True
    except:
        # TODO: implement handling for different exceptions
        #   i.e. If an instance has already been deleted, stop 
        #   trying to delete it.
        time.sleep(10)
        return False


def kill_jobs(event, context):
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

    duplicates = body['results'].get('nodes')
    if not duplicates:
        print("> No duplicates found; exiting.")
        return

    for duplicate in duplicates:
        name = duplicate['instanceName']
        zone = duplicate['zone']

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
        