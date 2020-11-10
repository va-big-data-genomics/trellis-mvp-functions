import os
import re
import json
import base64
import logging

from google.cloud import storage
from google.api_core import exceptions

ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    PROJECT_ID = os.environ['GCP_PROJECT']

    CLIENT = storage.Client()


def check_storage_class_request(extension, current_class, requested_class):

    supported_classes = {
                         "NEARLINE": "NEARLINE_STORAGE_CLASS",
                         "COLDLINE": "COLDLINE_STORAGE_CLASS",
                         "STANDARD": "STANDARD_STORAGE_CLASS",
                         "ARCHIVE" : "ARCHIVE_STORAGE_CLASS"
    }
    
    # Key = extension, value = requested storage class
    supported_updates = {
                         "fastq.gz": ["COLDLINE"],
    }

    if requested_class not in supported_classes.keys():
        logging.error(f"Storage class {requested_class} is not supported.")
        return False

    if supported_updates.get(extension):
        if requested_class in supported_updates[extension]:
            # Success case
            return supported_classes[requested_class]
        else:
            logging.error(f"Storage class {requested_class} is not supported for {extension}.")
            return False
    else:
        logging.error(f"Blob type \"{extension}\" is not supported.")
        return False

def update_storage_class(client, bucket, path, storage_class, dry_run):
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(path)

    if not dry_run:
        blob.update_storage_class(storage_class)
    return True

def main(event, context):
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

    # Used for local testing
    if header.get('dry-run') == 'True':
        dry_run = True
    else:
        dry_run = False

    nodes = body['results']
    if not nodes:
        print("> No node metadata found; exiting.")
        return  

    blob_counter = 0
    for node in nodes:

        bucket_name = node['bucket']
        blob_path = node['path']
        extension = node['extension']
        current_class = node['current_class']
        requested_class = node['requested_class']

        storage_class = check_storage_class_request(extension, current_class, requested_class)
        if not storage_class:
            logging.error(f"> Invalid storage class request. {extension}, {current_class}, {requested_class}.") 
            return

        logging.info(f"> Attempting to change storage class for gs://{bucket_name}/{blob_path}.")     
        storage_change = update_storage_class(
                                   client = CLIENT,
                                   bucket = bucket_name,
                                   path = blob_path,
                                   storage_class = storage_class,
                                   dry_run = dry_run)
        
        if storage_change:
            logging.info(f"> Storage class updated.")
            blob_counter +=1
    logging.info(f"> Count of blobs updated: {blob_counter}.")
    return blob_counter
        