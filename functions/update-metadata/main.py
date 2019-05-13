import os
import json
import base64

from google.cloud import storage

ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    PROJECT_ID = os.environ.get('PROJECT_ID')

    CLIENT = storage.Client(project=PROJECT_ID)

def update_metadata(event, context):
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
    print(context)
    print(context.resource)

    # Parse the message
    #elements = pubsub_message.split('#')
    blob_metadata = json.loads(data)

    if blob_metadata.get('resource') != 'blob':
        print(f"Error: Expected resource type 'blob', " +
              f"got '{blob_data['resource']}.'")
        return

    bucket_name = blob_metadata['gcp-metadata']['bucket']
    name = blob_metadata['gcp-metadata']['name']

    # Get blob & update metadata
    bucket = CLIENT.get_bucket(bucket_name)
    blob = bucket.get_blob(name)
    blob.metadata = {'gcf-update-metadata': event_id}
    blob.patch()