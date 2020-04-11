#import neo4j
import os
import re 
import json
import yaml

from py2neo import Graph

from google.cloud import storage
from google.cloud import pubsub

# Get environment variables
ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']

    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)
  
    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    READ_BUCKET_NAME = parsed_vars['TRELLIS_BUCKET']
    READ_PREFIX = parsed_vars['MATCHED_BLOBS_PREFIX']
    DATA_BUCKETS = parsed_vars['DATA_BUCKETS']
    PUBLISH_TOPIC = parsed_vars['TOPIC_UPDATE_METADATA']

    NEO4J_SCHEME = parsed_vars['NEO4J_SCHEME']
    NEO4J_HOST = parsed_vars['NEO4J_HOST']
    NEO4J_USER = parsed_vars['NEO4J_USER']
    NEO4J_PASS = parsed_vars['NEO4J_PASSPHRASE']
    NEO4J_PORT = parsed_vars['NEO4J_PORT']

    # Establish Graph connection
    GRAPH = Graph(
                  scheme=NEO4J_SCHEME,
                  host=NEO4J_HOST, 
                  port=NEO4J_PORT,
                  user=NEO4J_USER, 
                  password=NEO4J_PASS)

    # Establish PubSub connection
    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = PUBLISHER.topic_path(PROJECT_ID, PUBLISH_TOPIC)

def query_db_index(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    new_object = event
    print(f"Processing bucket object: {new_object['name']}.")

    # Check if file path matches read pattern
    object_name = new_object['name']
    match = re.match(READ_PREFIX, object_name)
    if not match:
        print(
              'Warning: object name did not match read prefix. ' + 
              f'Object name: {object_name}. ' + 
              f'Read prefix: {READ_PREFIX}.')
        return None
    
    # Check whether bucket in read path is tracked by this db
    approved_buckets = DATA_BUCKETS
    
    name_suffix = re.split(READ_PREFIX, object_name)[1]
    data_bucket_name = name_suffix.split('/')[1]
    if not data_bucket_name in approved_buckets:
        print(
              f'Info: bucket {data_bucket_name} ' + 
              'not tracked by this function database.')
        return None

    # Load file as string
    client = storage.Client(project=PROJECT_ID)
    read_bucket = client.get_bucket(READ_BUCKET_NAME)
    read_blob = read_bucket.get_blob(object_name)
    list_blobs_page = read_blob.download_as_string().decode('utf-8')
    list_blobs = json.loads(list_blobs_page)

    #property_dicts = {}
    property_dicts = []
    for blob_metadata in list_blobs:
        # Not using md5 anymore: https://cloud.google.com/storage/docs/hashes-etags
        if blob_metadata.get('resource') != 'blob':
            print(f"Error: Expected resource type 'blob', " +
                  f"got '{blob_data['resource']}.'")
            return

        gcp_metadata = blob_metadata['gcp-metadata']
        try:
            bucket = gcp_metadata['bucket']
            path = gcp_metadata['name']
            size = gcp_metadata['size']
            crc32c = gcp_metadata['crc32c']
        except:
            print(f'Error: Blob missing required metadata; skipping. ' + 
                  f'Blob metadata: {blob_metadata}.')
            continue

        trellis_metadata = blob_metadata['trellis-metadata']
        try:
            labels = trellis_metadata['labels']
            # Arbitrarily choose a label index to query against
            label = labels[0]
        except:
            print(
                  f'Error: Could not get node label. ' + 
                  f'Metadata: {blob_metadata}.')
            continue

        property_dicts.append(gcp_metadata)

    graphed_paths = []
    query = (
             'WITH {data} as entries ' + 
             'UNWIND entries as entry ' +
             'MATCH (n:Blob ' +
                                '{bucket: entry.bucket, ' +
                                 'path: entry.name, ' + 
                                 'size: toInteger(entry.size), ' + 
                                 'id: entry.id, ' + 
                                 'crc32c: entry.crc32c}) ' +
             'RETURN id(n) AS id, n.path as path')
    print(f"Neo4j query: {query}.")

    result = GRAPH.run(query, data=property_dicts).data()
    #result = GRAPH.run(query, data=metadata).data()
    print(f'Database query results: {result}')

    # I want to get the property dict entries that do not have a corresponding graph result
    for entry in result:
        graphed_paths.append(entry['path'])

    # Publish metadata for blobs not found in database
    publish_counter = 0
    for blob_metadata in list_blobs:
        path = blob_metadata['gcp-metadata']['name']
        if not path in graphed_paths:
            # Send untracked objects to Pub/Sub topic
            data = json.dumps(blob_metadata).encode('utf-8')
            result = PUBLISHER.publish(TOPIC_PATH, data=data).result()
            publish_counter += 1
    print(f'Count of blobs published to {TOPIC_PATH}: {publish_counter}.')
    return
