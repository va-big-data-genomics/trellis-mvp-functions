import os
import json
import yaml
import base64
import importlib

from datetime import datetime

from google.cloud import storage
from google.cloud import pubsub

# Get environment variables
ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    project_id = parsed_vars['GOOGLE_CLOUD_PROJECT']
    write_bucket_name = parsed_vars['TRELLIS_BUCKET']
    write_prefix = parsed_vars['BUCKET_PAGE_PREFIX']
    publish_topic = parsed_vars['TOPIC_LIST_BUCKET_PAGE']
    approved_buckets = parsed_vars['DATA_BUCKETS']

    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(
                                      project_id, 
                                      publish_topic)   

    storage_client = storage.Client(project=project_id) 

def get_timestamp():
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d-%H%M")
    return timestamp

def list_bucket_page(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

    # Get JSON formatted message
    data = json.loads(pubsub_message)

    if data['resource'] != 'bucket':
        print(f"Error: Expected resource type 'bucket', " +
              f"got '{data['resource']}.'")
        return

    # Parse message
    gcp_metadata = data.get('gcp-metadata')
    trellis_metadata = data.get('trellis-metadata')
    if not trellis_metadata:
        trellis_metadata = {}

    read_bucket_name = gcp_metadata.get('name')
    prefix = gcp_metadata.get('prefix')
    page_index = gcp_metadata.get('page-index')
    token = gcp_metadata.get('page-token')

    # Check that bucket is approved for reading
    #approved_buckets = approved_buckets_str.split(',')
    if not read_bucket_name in approved_buckets:
        # TODO: Raise this as an error
        print(f"Error: Bucket {read_bucket_name} is not approved for reading.'")
        return
    
    timestamp = trellis_metadata.get('timestamp')
    if not timestamp:
        timestamp = get_timestamp()

    if not page_index:
        page_index = 1

    # Get list_blobs() page
    read_bucket = storage_client.get_bucket(read_bucket_name)
    write_bucket = storage_client.get_bucket(write_bucket_name)

    iterator = read_bucket.list_blobs(
                                      page_token = token, 
                                      prefix = prefix)
    
    # Get metadata for objects just on this page
    page = next(iterator.pages)
    
    # Publish next page data back to Pub/Sub topic
    next_token = iterator.next_page_token
    next_index = page_index + 1

    # Publish next page to pubsub
    if next_token:
        next_data = {
                     "resource": "bucket", 
                     "gcp-metadata": {
                                      "name": read_bucket_name, 
                                      "prefix": prefix, 
                                      "page-index": next_index, 
                                      "page-token": next_token
                     }, 
                     "trellis-metadata": {
                                          "timestamp": timestamp
                     }
        }
        # Publish to topic
        next_data = json.dumps(next_data).encode('utf-8')
        publisher.publish(topic_path, data=next_data)

    page_data = []
    for blob in page:

        blob_data = {
                    "resource": "blob", 
                    "gcp-metadata": {
                                     "bucket": read_bucket.name, 
                                     "name": blob.name, 
                                     "size": str(blob.size), 
                                     "md5Hash": blob.md5_hash, 
                                     "crc32c": blob.crc32c, 
                                     "id": blob.id, 
                    }, 
                    "trellis-metadata": {
                                         "timestamp": timestamp
                    }
        }
        page_data.append(blob_data)
    print(f"Number of blobs listed from page: {len(page_data)}.")

    # Write blob metadata to a GCS object
    output_path = f'{write_prefix}/{read_bucket.name}/{timestamp}/{page_index}.txt'
    out_object = write_bucket.blob(output_path)

    page_str = json.dumps(page_data)
    out_object.upload_from_string(page_str)

if __name__ == "__main__": 
    project_id = "gbsc-gcp-project-mvp"
    write_bucket_name = "gbsc-gcp-project-mvp-trellis-test"
    write_prefix = "trellis/list-bucket-page"
    publish_topic = "bucket-page-tokens"
    approved_buckets_str = 'gbsc-gcp-project-mvp-group'

    topic_path = publisher.topic_path(
                                  project_id, 
                                  publish_topic)   

    data = {
            "resource": "bucket", 
            "gcp-metadata": {
                             "name": "gbsc-gcp-project-mvp-group"
            }
    }
    data = json.dumps(data)
    data = data.encode('utf-8')
    event = {'data': base64.b64encode(data)}

    context = None

    main(event, context)

