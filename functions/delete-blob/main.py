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
    
def check_protected_patterns(path):

    # Include the patterns within this function so that unit tests
    # are consistent with runtime execution.
    protected_patterns = [
                          "fastq.gz",
                          "g.vcf.gz$",
                          "g.vcf.gz.tbi$",
                          ".cram$",
                          ".cram.crai$",
                          "flagstat.data.tsv$",
                          "fastqc.data.txt$",
                          "vcfstats.data.txt$",
    ]

    for pattern in protected_patterns:
        if re.search(pattern, path):
            return True
    return False
 

def delete_blob(client, bucket, path, dry_run):
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(path)

    try:
        if not dry_run:
            blob.delete()
    except exceptions.NotFound as e:
        logging.warning(f"> Blob has already been deleted.")
        return False
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

        protected_status = check_protected_patterns(node['path'])
        if protected_status:
            logging.error(f"> Attempted to delete protected object. Aborting. {pattern}: {path}.")

        logging.info(f"> Attempting to delete blob gs://{node['bucket']}/{node['path']}.")     
        blob_deleted = delete_blob(
                                   client = CLIENT,
                                   bucket = node["bucket"],
                                   path = node["path"],
                                   dry_run = dry_run)
        
        if blob_deleted:
            logging.info(f"> Blob deleted.")
            blob_counter +=1
    logging.info(f"> Count of blobs deleted: {blob_counter}.")
    return blob_counter
        