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

    nodes = body['results']
    if not nodes:
        print("> No node metadata found; exiting.")
        return  

    # Hardcode protections against deleted essential data types
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

    blob_counter = 0
    for node in nodes:
        for pattern in protected_patterns:
            if re.search(pattern, node['path']):
                logging.error(f"> Attempted to delete protected object. Aborting. {pattern}: {node['path']}.")
                return

        logging.info(f"> Attempting to delete blob gs://{node['bucket']}/{node['path']}.")
        
        bucket = CLIENT.get_bucket(node["bucket"])
        blob = bucket.blob(node["path"])

        try:
            blob.delete()
        except exceptions.NotFound as e:
            logging.warning(f"> Blob has already been deleted.")
            continue
        
        logging.info(f"> Blob deleted.")
        blob_counter +=1
    logging.info(f"> Count of blobs deleted: {blob_counter}.")
        