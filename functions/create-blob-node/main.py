import os
import re
import pdb
import json
import pytz
import yaml
import iso8601
import importlib

from datetime import datetime

from google.cloud import storage
from google.cloud import pubsub

# Get runtime variables from cloud storage bucket
# https://www.sethvargo.com/secrets-in-serverless/
ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    TRIGGER_OPERATION = os.environ['TRIGGER_OPERATION']
    GIT_COMMIT_HASH = os.environ['GIT_COMMIT_HASH']
    GIT_VERSION_TAG = os.environ['GIT_VERSION_TAG']

    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    # Runtime variables
    PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT')
    DB_QUERY_TOPIC = parsed_vars.get('DB_QUERY_TOPIC')
    TOPIC_TRIGGERS = parsed_vars.get('TOPIC_TRIGGERS')
    DATA_GROUP = parsed_vars.get('DATA_GROUP')
    FUNC_GROUP = parsed_vars.get('FUNC_GROUP')

    PUBLISHER = pubsub.PublisherClient()


def format_pubsub_message(query, seed_id):
    message = {
               "header": {
                          "resource": "query",
                          "method": "POST",
                          "labels": ["Create", "Blob", "Node", "Cypher", "Query"],
                          "sentFrom": f"{FUNCTION_NAME}",
                          "publishTo": f"{TOPIC_TRIGGERS}",
                          "seedId": f"{seed_id}",
                          "previousEventId": f"{seed_id}"
               },
               "body": {
                        "cypher": query,
                        "result-mode": "data",
                        "result-structure": "list",
                        "result-split": "True",
               },
    }
    return message


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result


def clean_metadata_dict(raw_dict):
    """Remove dict entries where the value is of type dict"""
    clean_dict = dict(raw_dict)

    # Remove values that are dicts
    delete_keys = []
    for key, value in clean_dict.items():
        if isinstance(value, dict):
            #del clean_dict[key]
            delete_keys.append(key)

    for key in delete_keys:
        del clean_dict[key]

    # Convert size field from str to int
    clean_dict['size'] = int(clean_dict['size'])

    return clean_dict


def get_standard_name_fields(event_name, event_bucket):
    """(pbilling 200226): This should probably be moved to config file.
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
                   "gitCommitHash": GIT_COMMIT_HASH,
                   "gitVersionTag": GIT_VERSION_TAG,
                   "uri" : "gs://" + event_bucket + "/" + event_name,
    }
    return name_fields


def get_standard_time_fields(event):
    """
    Args:
        event (dict): Metadata properties stored as strings
    Return
        (dict): Times in iso (str) and from-epoch (int) formats
    """
    datetime_created = get_datetime_iso8601(event['timeCreated'])
    datetime_updated = get_datetime_iso8601(event['updated'])


    time_created_epoch = get_seconds_from_epoch(datetime_created)
    time_created_iso = datetime_created.isoformat()

    time_updated_epoch = get_seconds_from_epoch(datetime_updated)
    time_updated_iso = datetime_updated.isoformat()

    time_fields = {
                   'timeCreatedEpoch': time_created_epoch,
                   'timeUpdatedEpoch': time_updated_epoch,
                   'timeCreatedIso': time_created_iso,
                   'timeUpdatedIso': time_updated_iso
    }
    return time_fields


def get_seconds_from_epoch(datetime_obj):
    """Get datetime as total seconds from epoch.

    Provides datetime in easily sortable format

    Args:
        datetime_obj (datetime): Datetime.
    Returns:
        (float): Seconds from epoch
    """
    from_epoch = datetime_obj - datetime(1970, 1, 1, tzinfo=pytz.UTC)
    from_epoch_seconds = from_epoch.total_seconds()
    return from_epoch_seconds


def get_datetime_iso8601(date_string):
    """ Convert ISO 86801 date strings to datetime objects.

    Google datetime format: https://tools.ietf.org/html/rfc3339
    ISO 8601 standard format: https://en.wikipedia.org/wiki/ISO_8601

    Args:
        date_string (str): Date in ISO 8601 format
    Returns
        (datetime.datetime): Datetime objects
    """
    return iso8601.parse_date(date_string)


def format_node_merge_query(db_dict, dry_run=False):
    # Create label string
    tmp_labels = list(db_dict['labels'])
    tmp_labels.remove('Blob')
    labels_str = ':'.join(tmp_labels)

    # Create database ON CREATE string
    create_strings = []
    for key, value in db_dict.items():
        if isinstance(value, str):
            create_strings.append(f'node.{key} = "{value}"')
        else:
            create_strings.append(f'node.{key} = {value}')
    create_string = ', '.join(create_strings)

    # Create database ON MATCH string
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
                  'storageClass']

    merge_strings = []
    for key in merge_keys:
        value = db_dict.get(key)
        if value:
            if isinstance(value, str):
                merge_strings.append(f'node.{key} = "{value}"')
            else:
                merge_strings.append(f'node.{key} = {value}')
    merge_string = ', '.join(merge_strings)

    query = (
        f"MERGE (node:Blob:{labels_str} {{ " +
            f'bucket: "{db_dict["bucket"]}", ' +
            f'path: "{db_dict["path"]}" }}) ' +
        "ON CREATE SET node.nodeCreated = timestamp(), " +
            'node.nodeIteration = "initial", ' +
            f"{create_string} " +
        f"ON MATCH SET " +
            'node.nodeIteration = "merged", ' +
            f"{merge_string} " +
        "RETURN node")
    return query


def create_node_query(event, context):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    print(f"> Processing new object event: {event['name']}.")
    print(f"> Event: {event}).")
    print(f"> Context: {context}.")

    seed_id = context.event_id

    # Trellis config data
    name = event['name']
    bucket_name = event['bucket']

    # Module name does not include project prefix
    pattern = f"{PROJECT_ID}-(?P<suffix>\w+(?:-\w+)+)"
    match = re.match(pattern, bucket_name)
    suffix = match['suffix']

    # Import the config modules that corresponds to event-trigger bucket
    node_module_name = f"{DATA_GROUP}.{suffix}.create-node-config"
    node_module = importlib.import_module(node_module_name)

    node_kinds = node_module.NodeKinds()
    label_patterns = node_kinds.match_patterns

    # Create dict of metadata to add to database node
    gcp_metadata = event
    db_dict = clean_metadata_dict(event)

    # Add standard fields
    name_fields = get_standard_name_fields(event['name'], event['bucket'])
    time_fields = get_standard_time_fields(event)

    db_dict.update(name_fields)
    db_dict.update(time_fields)

    # Add trigger operation as metadata property
    db_dict['triggerOperation'] = TRIGGER_OPERATION

    # Check db_dict with metadata about object
    db_dict['labels'] = []
    for label, patterns in label_patterns.items():
        for pattern in patterns:
            match = re.fullmatch(pattern, name)
            if match:
                db_dict['labels'].append(label)
                label_functions = node_kinds.label_functions.get(label)
                if label_functions:
                    for function in label_functions:
                        custom_fields = function(db_dict, match.groupdict())
                        db_dict.update(custom_fields)
                # Break after a single pattern per label has been matched
                break

    # Ignore log files
    log_labels = set(['Log', 'Stderr', 'Stdout'])
    log_intersection = log_labels.intersection(db_dict['labels'])
    if log_intersection:
        print(f"> This is a log file; ignoring. {db_dict['labels']}")
        return

    # Key, value pairs unique to db_dict are trellis metadata
    trellis_metadata = {}
    for key, value in db_dict.items():
        if not key in gcp_metadata.keys():
            trellis_metadata[key] = value

    print(f"> Generating database query for node: {db_dict}.")
    db_query = format_node_merge_query(db_dict)
    print(f"> Database query: \"{db_query}\".")

    message = format_pubsub_message(db_query, seed_id)
    print(f"> Pubsub message: {message}.")
    result = publish_to_topic(DB_QUERY_TOPIC, message)
    print(f"> Published message to {DB_QUERY_TOPIC} with result: {result}.")