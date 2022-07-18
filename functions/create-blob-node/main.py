import os
import re
import pdb
import json
import uuid
import yaml
import iso8601
import importlib

import trellisdata as trellis

from collections import deque

from google.cloud import storage
from google.cloud import pubsub

ENVIRONMENT = os.environ.get('ENVIRONMENT', 'Environment variable not set')
if ENVIRONMENT == 'google-cloud':

    # set up the Google Cloud Logging python client library
    # source: https://cloud.google.com/blog/products/devops-sre/google-cloud-logging-python-client-library-v3-0-0-release
    import google.cloud.logging
    client = google.cloud.logging.Client()
    client.setup_logging()

    # use Python's standard logging library to send logs to GCP
    import logging

    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    TRIGGER_OPERATION = os.environ['TRIGGER_OPERATION']
    GIT_COMMIT_HASH = os.environ['GIT_COMMIT_HASH']
    GIT_VERSION_TAG = os.environ['GIT_VERSION_TAG']
    GCP_PROJECT = os.environ['GCP_PROJECT']

    config_doc = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    # https://stackoverflow.com/questions/6866600/how-to-parse-read-a-yaml-file-into-a-python-object
    TRELLIS = yaml.safe_load(config_doc)

    PUBLISHER = pubsub.PublisherClient()
    STORAGE_CLIENT = storage.Client()

    # Need to pull this from GCS
    label_taxonomy = storage.Client() \
                        .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                        .get_blob(TRELLIS['LABEL_TAXONOMY']) \
                        .download_as_string()

    TAXONOMY_PARSER = trellis.utils.TaxonomyParser()
    TAXONOMY_PARSER.read_from_string(label_taxonomy)
else:
    import logging

    TAXONOMY_PARSER = trellis.utils.TaxonomyParser()
    TAXONOMY_PARSER.read_from_json('label-taxonomy.json')


def clean_metadata_dict(raw_dict):
    """Remove dict entries where the value is of type dict"""
    clean_dict = dict(raw_dict)

    uuid = clean_dict['metadata'].get('trellis-uuid')
    clean_dict['trellisUuid'] = uuid

    # What if I just convert to strings
    # Remove values that are dicts
    delete_keys = []
    for key, value in clean_dict.items():
        if isinstance(value, dict):
            #del clean_dict[key]
            #delete_keys.append(key)
            clean_dict[key] = str(value)

    #for key in delete_keys:
    #    del clean_dict[key]

    # Convert size field from str to int
    clean_dict['size'] = int(clean_dict['size'])

    return clean_dict

def get_name_fields(event_name, event_bucket, commit_hash, version_tag):
    """(pbilling 200226): This should probably be moved to config file.

    Example input:
        event_name: va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz
        event_bucket: gcp-bucket-mvp-test-from-personalis
    
    Example output:
        path: va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz
        dirname: va_mvp_phase2/PLATE0/SAMPLE0/FASTQ
        basename: SAMPLE0_0_R1.fastq.gz
        name: SAMPLE0_0_R1
        extension: fastq.gz
        filetype: gz
        uri: gs://gcp-bucket-mvp-test-from-personalis/va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz
    """
    path_elements = event_name.split('/')
    name_elements = path_elements[-1].split('.')
    name_fields = {
                   "path": event_name,
                   "dirname": '/'.join(path_elements[:-1]),
                   "basename": path_elements[-1],
                   "name": name_elements[0],
                   "extension": '.'.join(name_elements[1:]),
                   "filetype": name_elements[-1],
                   "gitCommitHash": commit_hash,
                   "gitVersionTag": version_tag,
                   "uri" : "gs://" + event_bucket + "/" + event_name,
    }
    return name_fields

def get_time_fields(event):
    """
    Args:
        event (dict): Metadata properties stored as strings
    Return
        (dict): Times in iso (str) and from-epoch (int) formats
    """

    # Google datetime format: https://tools.ietf.org/html/rfc3339
    # ISO 8601 standard format: https://en.wikipedia.org/wiki/ISO_8601
    datetime_created = iso8601.parse_date(event['timeCreated'])
    datetime_updated = iso8601.parse_date(event['updated'])


    time_created_epoch = trellis.utils.get_seconds_from_epoch(datetime_created)
    time_created_iso = datetime_created.isoformat()

    time_updated_epoch = trellis.utils.get_seconds_from_epoch(datetime_updated)
    time_updated_iso = datetime_updated.isoformat()

    time_fields = {
                   'timeCreatedEpoch': time_created_epoch,
                   'timeUpdatedEpoch': time_updated_epoch,
                   'timeCreatedIso': time_created_iso,
                   'timeUpdatedIso': time_updated_iso
    }
    return time_fields

def create_parameterized_merge_query(label, query_parameters):

    create_strings = []
    for key in query_parameters.keys():
        create_strings.append(f'node.{key} = ${key}')
    create_string = ', '.join(create_strings)

    # If node already exists in the database, only update the following
    # values of the node (SET command), if the values are provided
    merge_keys = [
                  'md5Hash',
                  'size',
                  'timeUpdatedEpoch',
                  'timeUpdatedIso',
                  'timeStorageClassUpdated',
                  'updated',
                  'id',
                  'crc32c',
                  'generation',
                  'storageClass',
                  # Following are specific to Checksum objects
                  'fastqCount',
                  'microarrayCount']

    merge_strings = []
    for key in merge_keys:
        value = query_parameters.get(key)
        if value:
            merge_strings.append(f'node.{key} = ${key}')
    merge_string = ', '.join(merge_strings)

    parameterized_query = (
        f"MERGE (node:{label} {{ uri: $uri, crc32c: $crc32c }}) " +
        "ON CREATE SET node.nodeCreated = timestamp(), " +
            "node.nodeIteration = 'initial', " +
            f"{create_string} " +
        "ON MATCH SET " +
            "node.nodeIteration = 'merged', " + 
            f"{merge_string} " +
        "RETURN node")
    return parameterized_query

def add_uuid_to_blob(bucket, path):
    """For a json object, get and return json data.

    Args:
        bucket (str): Name of Google Cloud Storage (GCS) bucket.
        path (str): Path to GCS object.
    Returns:
        Blob.metadata (dict)
    """
    metadata = {'trellis-uuid': uuid.uuid4()}

    storage_client = storage.Client()
    bucket = STORAGE_CLIENT.get_bucket(bucket)
    blob = bucket.get_blob(path)
    blob.metadata = metadata
    blob.patch()

    return blob.metadata

def assign_labels(path, label_match_patterns):
    """Used for testing"""
    labels = []
    for label, patterns in label_match_patterns.items():
        for pattern in patterns:
            match = re.fullmatch(pattern, path)
            if match:
                labels.append(label)
    return labels

def assign_labels_and_metadata(query_parameters, label_patterns, label_functions):
    #query_parameters['labels'] = []
    labels = []
    for label, patterns in label_patterns.items():
        for pattern in patterns:
            match = re.fullmatch(pattern, query_parameters['path'])
            if match:
                labels.append(label)
                #query_parameters['labels'].append(label)
                metadata_functions = label_functions.get(label)
                if metadata_functions:
                    for metadata_function in metadata_functions:
                        custom_fields = metadata_function(query_parameters, match.groupdict())
                        query_parameters.update(custom_fields)
                # Break after a single pattern per label has been matched
                # According to single-label mode, objects can't/shouldn't(?)
                # match more than one label.
                break
    return query_parameters, labels

def get_leaf_labels(labels, taxonomy_parser):
    # Get only the shallowest labels of a branch of the taxonomy that should be applied 
    # to the node. The point of the taxonomy is so that we can retain lineage information
    # without applying multiple labels to a node.
    common_parents = []
    for label in labels:
        node = taxonomy_parser.find_by_name(label)
        # https://docs.python.org/3/library/collections.html#collections.deque
        parents = deque(node.path)

        parents.popleft() # Remove the arbitrary root node
        parents.pop()     # Remove the current label
        common_parents.extend(parents)
    common_parents = set(common_parents) # Only keep unique nodes
    
    # If a label is a parent of another label, exclude it
    for label in labels:
        if label in [parent.name for parent in common_parents]:
            labels.remove(label)
    return labels

def create_node_query(event, context, test=False):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    logging.info(f"> Processing new object event: {event['name']}.")
    logging.info(f"> Event: {event}.")
    logging.info(f"> Context: {context}.")

    seed_id = context.event_id

    # Trellis config data
    name = event['name']
    bucket_name = event['bucket']

    logging.info(f"> Environment: {ENVIRONMENT}.")
    if ENVIRONMENT == 'google-cloud':
        # Use bucket name to determine which config file should be used
        # to parse object metadata.
        # (Module name does not include project prefix)
        pattern = f"{GCP_PROJECT}-(?P<suffix>\w+(?:-\w+)+)"
        match = re.match(pattern, bucket_name)
        suffix = match['suffix']

        # TODO: Create a separate instance of create-blob-node for each bucket
        #   and include the module in the function deployment parameters,
        #   controlled by Terraform.
        # Import the config module that corresponds to event-trigger bucket
        node_module_name = f"{TRELLIS['DATA_GROUP']}.{suffix}.create-node-config"
        node_module = importlib.import_module(node_module_name)
    else:
        import test_create_node_config as node_module

    node_kinds = node_module.NodeKinds()
    label_patterns = node_kinds.match_patterns
    label_functions = node_kinds.label_functions
    logging.info(f"> Label patterns: {len(label_patterns)}, label functions: {len(label_functions)}.")

    # Create dict of metadata to add to database node
    gcp_metadata = event
    query_parameters = clean_metadata_dict(event)
    logging.info(f"> Cleaned object metadata: {query_parameters}.")

    # Add standard fields
    name_fields = get_name_fields(
                    event_name = event['name'], 
                    event_bucket = event['bucket'],
                    commit_hash = GIT_COMMIT_HASH,
                    version_tag = GIT_VERSION_TAG)
    time_fields = get_time_fields(event)

    query_parameters.update(name_fields)
    query_parameters.update(time_fields)
    logging.info(f"> Query parameters: {query_parameters}.")

    # Generate UUID
    if not query_parameters.get('trellisUuid') and ENVIRONMENT == 'google-cloud':
        uuid = add_uuid_to_blob(
                                query_parameters['bucket'], 
                                query_parameters['path'])
        logging.info(f"> Object UUID added: {uuid}. Exiting.")
        return # Updating metadata will trigger this function again

    # Add trigger operation as metadata property
    query_parameters['triggerOperation'] = TRIGGER_OPERATION

    # Populate query_parameters with metadata about object
    logging.info(f"> Query parameter 'path': {query_parameters['path']}.")
    query_parameters, labels = assign_labels_and_metadata(query_parameters, label_patterns, label_functions)
    logging.info(f"> Labels assigned to node: {labels}.")

    labels = get_leaf_labels(labels, TAXONOMY_PARSER)
    logging.info(f"> Leaf labels (expect one): {labels}.")

    if 'Log' in labels:
        logging.info(f"> This is a log file; ignoring.")
        return

    # Max (1) label per node to choose parameterized query
    if len(labels) > 1:
        logging.error(f"> More than one label applied to node: [{labels}].")
    elif not labels:
        logging.error("> No labels applied to node.")
    else:
        label = labels[0]

    # Generate UUID
    # NOTE: This reactivates the function and creates an infinte loop 
    # because also it's activating every time
    if not query_parameters.get('trellisUuid') and ENVIRONMENT == 'google-cloud':
        uuid = add_uuid_to_blob(
                                query_parameters['bucket'], 
                                query_parameters['path'])
        logging.info("> The metadata for the blob {} is {}".format(blob.name, blob.metadata))
        query_parameters['trellisUuid'] = blob.metadata['uuid']


    # Dynamically create parameterized query
    parameterized_query = create_parameterized_merge_query(label, query_parameters)

    #print(f"> Generating database query for node: {db_dict}.")
    #db_query = format_node_merge_query(db_dict)
    #print(f"> Database query: \"{db_query}\".")

    query_request = trellis.QueryRequestWriter(
        sender = FUNCTION_NAME,
        seed_id = seed_id,
        previous_event_id = seed_id,
        query_name = f"mergeBlob{label}",
        query_parameters = query_parameters,
        custom = True,
        cypher = parameterized_query,
        write_transaction = True,
        aggregate_results = False,
        publish_to = ["TOPIC_TRIGGERS"],
        returns = {
                   "pattern": "node",
                   "start": label
        })
    message = query_request.format_json_message()
    logging.info(f"> Topic: {TRELLIS['TOPIC_DB_QUERY']}, message: {message}.")
    if ENVIRONMENT == 'google-cloud':
        result = trellis.utils.publish_to_pubsub_topic(
            publisher = PUBLISHER,
            project_id = GCP_PROJECT,
            topic = TRELLIS['TOPIC_DB_QUERY'],
            message = message)
        logging.info(f"> Published message to {TRELLIS['TOPIC_DB_QUERY']} with result: {result}.")
    else:
        logging.warning("> Could not determine environment. Message was not published.")
        return(message)