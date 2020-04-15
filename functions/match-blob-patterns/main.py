import os
import re
import json
import yaml
import importlib

from google.cloud import storage

# Get environment variables
ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    READ_BUCKET_NAME = parsed_vars['TRELLIS_BUCKET']
    READ_PREFIX = parsed_vars['BUCKET_PAGE_PREFIX']
    WRITE_BUCKET_NAME = parsed_vars['TRELLIS_BUCKET']
    WRITE_PREFIX = parsed_vars['MATCHED_BLOBS_PREFIX']
    DATA_GROUP = parsed_vars['DATA_GROUP']

    client = storage.Client(project=PROJECT_ID)
    read_bucket = client.get_bucket(READ_BUCKET_NAME)

def match_blob_patterns(event, context):
    """Check whether object paths match any node patterns.

    Check whether object paths match any regex patterns for
    database nodes. If so, add a node label, for querying 
    against database index, and write to output.

    Triggered by a finalized object in Cloud Storage bucket.
    
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
              'Info: ignoring new bucket object: ' + 
              f'{object_name}. ' + 
              f'Read prefix: {READ_PREFIX}.')
        return None

    # Get name of bucket that blobs are being listed for
    elements = object_name.split('/')
    list_bucket_name = elements[-3]
    timestamp = elements[-2]
    index = elements[-1].split('.')[0]
    
    # Load file as JSON
    read_blob = read_bucket.get_blob(object_name)
    list_blobs_page = read_blob.download_as_string().decode('utf-8')
    list_blobs = json.loads(list_blobs_page)

    # Import the config module that corresponds to listed-objects bucket
    #meta_module_name = f'{DATA_GROUP}.{list_bucket_name}.create-node-config'
    #meta_module = importlib.import_module(meta_module_name)

    # Module name does not include project prefix
    pattern = f"{PROJECT_ID}-(?P<suffix>\w+(?:-\w+)+)"
    match = re.match(pattern, list_bucket_name)
    suffix = match['suffix']

    # Import the config modules that corresponds to event-trigger bucket
    node_module_name = f"{DATA_GROUP}.{suffix}.create-node-config"
    node_module = importlib.import_module(node_module_name)

    node_kinds = node_module.NodeKinds()
    label_patterns = node_kinds.match_patterns
    #kind_matches = {}

    matched_blobs = []
    for blob_metadata in list_blobs:

        # QA that blob has appropriate metadat fields
        if blob_metadata.get('resource') != 'blob':
            print(f"Error: expected resource type 'blob', " +
                  f"got '{blob_data['resource']}.'")
            return

        name = blob_metadata.get('gcp-metadata').get('name')
        if not name:
            print("Error: blob metadata does not include name. " + 
                  f"Blob dict: {blob_metadata}.")
            return

        # Determine which kind patterns match the object name
        node_labels = []
        for key, values in label_patterns.items():
            for pattern in values:
                match = re.fullmatch(pattern, name)
                if match:
                    # Only match one pattern per kind
                    node_labels.append(key)
                    break
        
        # Add kind labels to blob metdata
        if node_labels and "Blob" in node_labels:
            #node_labels.extend(global_labels)
            trellis_metadata = blob_metadata.get('trellis-metadata')
            if trellis_metadata:
                trellis_metadata['labels'] = node_labels
            else:
                blob_metadata['trellis_metadata'] = {'labels': node_labels}
            matched_blobs.append(blob_metadata)
        else:
            print(
                  'Warning: blob did not match any patterns. ' +
                  f'{blob_metadata}.')


    # Write GCS output objects for each kind
    write_bucket = client.get_bucket(WRITE_BUCKET_NAME)
    output_path = f'{WRITE_PREFIX}/{list_bucket_name}/{timestamp}/{index}.txt'
    output_obj = write_bucket.blob(output_path)
    output_str = json.dumps(matched_blobs)
    output_obj.upload_from_string(output_str)
    print(f"Matching blobs' metadata written to {output_path}.")
