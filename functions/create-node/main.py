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

    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    # Runtime variables
    PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT')
    TOPIC = parsed_vars.get('DB_QUERY_TOPIC')
    DATA_GROUP = parsed_vars.get('DATA_GROUP')

    PUBLISHER = pubsub.PublisherClient()


def format_pubsub_message(query):
    message = {
               "header": {
                          "resource": "query", 
                          "method": "POST",
                          "labels": ["Cypher", "Query", "Node", "Create"],
                          "sentFrom": f"{FUNCTION_NAME}",
                          "publishTo": f"{DATA_GROUP}-add-relationships",
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


def get_standard_name_fields(event_name):
    path_elements = event_name.split('/')
    name_elements = path_elements[-1].split('.')
    name_fields = {
                   "path": event_name,
                   "dirname": '/'.join(path_elements[:-1]),
                   "basename": path_elements[-1],
                   "name": name_elements[0],
                   "extension": '.'.join(name_elements[1:])
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


def format_output_node_query(db_entry, dry_run=False):
    """DEPRECATED since relationship logic moved to own function.
    """
    # Create label string
    labels_str = ':'.join(db_entry['labels'])

    # Create database entry string
    entry_strings = []
    for key, value in db_entry.items():
        if isinstance(value, str):
            entry_strings.append(f'{key}: "{value}"')
        else:
            entry_strings.append(f'{key}: {value}')
    entry_string = ', '.join(entry_strings)

    # Format as cypher query
    query = (
             f"MATCH (jobNode) WHERE jobNode.taskId=\"{db_entry['taskId']}\" " +
             f"CREATE (jobNode)-[:OUTPUT]-> " +
             f"(node:{labels_str} {{ {entry_string} }}) " +
              "RETURN node")
    return query


def format_node_query(db_entry, dry_run=False):
    # Create label string
    labels_str = ':'.join(db_entry['labels'])

    # Create database entry string
    entry_strings = []
    for key, value in db_entry.items():
        if isinstance(value, str):
            entry_strings.append(f'{key}: "{value}"')
        else:
            entry_strings.append(f'{key}: {value}')
    entry_string = ', '.join(entry_strings)

    # Format as cypher query
    query = (
             f"CREATE (node:{labels_str} {{ {entry_string} }}) " +
              "RETURN node")
    return query


def format_relationship_query(node1, node2, name, orientation, properties):
    """NOT CURRENTLY IN USE
    Format cypher query to create relationship between nodes

    node1 (dict): Properties of node to match in db
    node2 (dict): Properties of node to match in db
    name (str): Name of relationship
    orientation (str): Orientation of relationship ["-", "->"]

    relationship example:
        {
            "name": "INPUT_TO",
            "direction": "to",
            "properties": {}
        }
    """

    query = (
             f"MATCH (a {{ {node1} }}), " +
             f"(b {{ {node2} }}) " + 
             f"CREATE (a)-[:{name} {{ {properties} }}]{orientation}(b)")
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

    # Trellis config data
    name = event['name']
    bucket_name = event['bucket']

    # Import the config modules that corresponds to event-trigger bucket
    node_module_name = f"{DATA_GROUP}.{bucket_name}.create-node-config"
    node_module = importlib.import_module(node_module_name)

    node_kinds = node_module.NodeKinds()
    label_patterns = node_kinds.match_patterns

    # Create dict of metadata to add to database node
    gcp_metadata = event
    db_dict = clean_metadata_dict(event)

    # Add standard fields
    name_fields = get_standard_name_fields(event['name'])
    time_fields = get_standard_time_fields(event)

    db_dict.update(name_fields)
    db_dict.update(time_fields)

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

    # Key, value pairs unique to db_dict are trellis metadata
    trellis_metadata = {}
    for key, value in db_dict.items():
        if not key in gcp_metadata.keys():
            trellis_metadata[key] = value

    print(f"> Generating database query for node: {db_dict}.")
    db_query = format_node_query(db_dict)
    print(f"> Database query: \"{db_query}\".")

    message = format_pubsub_message(db_query)
    print(f"> Pubsub message: {message}.")
    result = publish_to_topic(TOPIC, message)
    print(f"> Published message to {TOPIC_PATH} with result: {result}.")

    #summary = {
    #           "name": name, 
    #           "bucket": bucket_name, 
    #           "node-module-name": node_module_name, 
    #           "labels": db_dict
    #           "db-query": db_query,
    #}
    #return(summary)


if __name__ == "__main__":
    # Run unit tests in local
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"
    TOPIC = "wgs35-db-queries"
    DATA_GROUP = 'wgs35'

    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID,
                                                       topic = TOPIC)

    # fastq
    event = {
             'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis', 
             'componentCount': 32, 
             'contentType': 'application/octet-stream', 
             'crc32c': 'ftNG8w==', 
             'etag': 'CL3nyPj80uECEBE=', 
             'generation': '1555361455813565', 
             'id': 'gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565', 
             'kind': 'storage#object', 
             'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media', 
             'metadata': {'function-testing': '20190423:1217', 'gcf-update-metadata': '510893936442804'}, 
             'metageneration': '17', 
             'name': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz', 
             'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz', 
             'size': '5955984357', 
             'storageClass': 'REGIONAL', 
             'timeCreated': '2019-04-15T20:50:55.813Z', 
             'timeStorageClassUpdated': '2019-04-15T20:50:55.813Z', 
             'updated': '2019-04-23T19:17:53.205Z'
    }
    context = None

    summary = create_node_query(event, context)
    
    # Test event attributes
    assert summary['name'] == event['name']
    assert summary['bucket'] == event['bucket']
    
    # Test node and trigger modules
    assert summary['node-module-name'] == f"{DATA_GROUP}.{event['bucket']}.create-node-config"
    
    # Test labels
    try:
        expected_labels = ['Blob', 'WGS_35000', 'Fastq']
        assert sorted(summary['labels']) == sorted(expected_labels)
        print("> Labels test: Pass.")
    except:
        print(f"! Error: Node labels ({summary['labels']}) " +
              f"do not match expected ({expected_labels}).")

    # Test database query
    try:
        expected_query = 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "gbsc-gcp-project-mvp-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECEBE=", generation: "1555361455813565", id: "gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "17", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-04-23T19:17:53.205Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556047073.205, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-04-23T19:17:53.205000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, readGroup: 0}) RETURN node'
        assert summary['db-query'] == expected_query
        print("> Query test: Pass.")
    except:
        print("! Error: database query does not match expected.")
        pdb.set_trace()
    # Test pubsub message

    # ubam
    event = {'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis-gatk', 'contentType': 'application/octet-stream', 'crc32c': 'ZaJM+g==', 'etag': 'CPiFjqbVgOICEAI=', 'generation': '1556931361866488', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/objects/SHIP4946367_0.ubam/1556931361866488', 'kind': 'storage#object', 'md5Hash': 'Tgh+eyIiKe8TRWV6vohGJQ==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Fobjects%2FSHIP4946367_0.ubam?generation=1556931361866488&alt=media', 'metadata': {'test': '20190506:1148'}, 'metageneration': '2', 'name': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/objects/SHIP4946367_0.ubam', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Fobjects%2FSHIP4946367_0.ubam', 'size': '16871102587', 'storageClass': 'REGIONAL', 'timeCreated': '2019-05-04T00:56:01.866Z', 'timeStorageClassUpdated': '2019-05-04T00:56:01.866Z', 'updated': '2019-05-06T18:48:06.711Z'}
    context = None
    summary = create_node_query(event, context)

