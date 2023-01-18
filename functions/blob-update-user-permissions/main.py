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

class TrellisMessage:

    def __init__(self, event, context):
        """Parse Trellis messages from Pub/Sub event & context.

        Args:
            event (type):
            context (type):

        Message format:
            - context
                - event_id (required)
            - event
                - header
                    - sentFrom (required)
                    - method (optional)
                    - resource (optional)
                    - labels (optional)
                    - seedId (optional)
                    - previousEventId (optional)
                - body
                    - cypher (optional)
                    - results (optional)
        """
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)
        logging.info(f"> Context: {context}.")
        logging.info(f"> Data: {data}.")
        logging.info(f"> Context: {context}.")
        logging.info(f"> Data: {data}.")
        
        header = data['header']
        body = data['body']

        self.event_id = context.event_id
        self.seed_id = header.get('seedId')

        # If no seed specified, assume this is the seed event
        if not self.seed_id:
            self.seed_id = self.event_id

        self.header = data['header']
        self.body = data['body']

        self.results = {}
        if body.get('results'):
            self.results = body.get('results')

    def check_required_results(self, required_fields):

        # Default value == True; if any are missing return False
        result = True
        for field in required_result_fields:
            result = self.results.get(field)
            if not result:
                logging.error("Message results are missing required field: {field}.")
                result = False
        # Wait until all fields have been checked before returning result
        # If multiple fields are missing, function should announce all of them
        return result

def check_storage_class_request(extension, current_class, requested_class):

    #supported_classes = {
    #                     "NEARLINE": "NEARLINE_STORAGE_CLASS",
    #                     "COLDLINE": "COLDLINE_STORAGE_CLASS",
    #                     "STANDARD": "STANDARD_STORAGE_CLASS",
    #                     "ARCHIVE" : "ARCHIVE_STORAGE_CLASS"
    #}
    
    supported_classes = [
                         "NEARLINE",
                         "COLDLINE",
                         "STANDARD",
                         "ARCHIVE"
    ]

    # Key = extension, value = requested storage class
    supported_updates = {
                         "fastq.gz": ["COLDLINE"],
    }

    if requested_class not in supported_classes:
        logging.error(f"Storage class {requested_class} is not supported.")
        return False

    if supported_updates.get(extension):
        if requested_class in supported_updates[extension]:
            # Success case
            return True
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
    return storage_class

def update_acl_permissions(client, bucket, path, blah, blah):
    """
        Interacting with blobs/objects:
            https://googleapis.dev/python/storage/latest/blobs.html
        Updating ACL permissions:
            https://googleapis.dev/python/storage/latest/acl.html?highlight=acl#module-google.cloud.storage.acl

        Methods for granting/revoking roles:
            read: _ACLEntity.grant_read(), _ACLEntity.revoke_read()
            write: _ACLEntity.grant_write(), _ACLEntity.revoke_write()
            owner: _ACLEntity.grant_owner(), _ACLEntity.revoke_owner()
    """

def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    
    Update the metadata of a Blob specified by PubSub message.

    Args:
         event (dict): 'data' contains a string describing with the
                       bucket and name of a GCS blob in the format 
                       of : {bucket}#{name}
         context (google.cloud.functions.Context): Metadata for the event.
    """

    message = TrellisMessage(event, context)

    required_result_fields = [
                              "blob",
                              "userEmail",
                              "chRole"]
    fields_check = message.check_required_results(required_result_fields)
    if not fields_check:
        logging.error("Required results fields were missing.")
        return

    # Used for local testing
    if message.header.get('dry-run') == 'True':
        dry_run = True
    else:
        dry_run = False

    user_email = message.results["user_email"]
    blob = message.results["blob"]
    ch_role = message.results["chRole"]

    bucket_name = blob['bucket']
    blob_path = blob['path']
    extension = blob['extension']
    current_class = blob['current_class']
    requested_class = blob['requested_class']

    logging.info(f"> Attempting to update user permission on gs://{bucket_name}/{blob_path}.")     
    storage_change = update_storage_class(
                               client = CLIENT,
                               bucket = bucket_name,
                               path = blob_path,
                               storage_class = requested_class,
                               dry_run = dry_run)
    
    if storage_change:
        logging.info(f"> Storage class updated to {storage_change}.")
        #blob_counter +=1
    #logging.info(f"> Count of blobs updated: {blob_counter}.")
    #return blob_counter
        