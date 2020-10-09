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
                          "labels": ["Blob", "Deleted", "Cypher", "Query"],
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


def get_standard_time_fields(context):
    """
    Args:
        event (dict): Metadata properties stored as strings
    Return
        (dict): Times in iso (str) and from-epoch (int) formats
    """
    datetime_deleted = get_datetime_iso8601(context.timestamp)

    time_deleted_epoch = get_seconds_from_epoch(datetime_deleted)
    time_deleted_iso = datetime_deleted.isoformat()

    time_fields = {
                   'timeDeletedEpoch': time_deleted_epoch,
                   'timeDeletedIso': time_deleted_iso,
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

    query = (
        f"MATCH (node:Blob {{ " +
            f'id: "{db_dict["id"]}", ' +
            f'path: "{db_dict["path"]}" }}) ' +
        f"SET node.obj_timeDeleted = datetime({db_dict['timeDeletedIso']}), " +
            f"node.obj_timeDeletedEpoch = {db_dict['timeDeletedEpoch']}, " +
        "RETURN node")
    return query


def register_blob_deleted(event, context):
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
    blob_id = event['id']

    # Add standard fields
    db_dict = get_standard_time_fields(context)

    # Add trigger operation as metadata property
    db_dict['triggerOperation'] = TRIGGER_OPERATION

    # Ignore log files
    log_labels = set(['Log', 'Stderr', 'Stdout'])
    log_intersection = log_labels.intersection(db_dict['labels'])
    if log_intersection:
        print(f"> This is a log file; ignoring. {db_dict['labels']}")
        return

    print(f"> Generating database query for node: {db_dict}.")
    db_query = format_node_merge_query(db_dict)
    print(f"> Database query: \"{db_query}\".")

    message = format_pubsub_message(db_query, seed_id)
    print(f"> Pubsub message: {message}.")
    result = publish_to_topic(DB_QUERY_TOPIC, message)
    print(f"> Published message to {DB_QUERY_TOPIC} with result: {result}.")



if __name__ == "__main__":
    # Run unit tests in local
    PROJECT_ID = "***REMOVED***-dev"
    TOPIC = "wgs35-db-queries"
    DATA_GROUP = 'wgs35'

    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID,
                                                       topic = TOPIC)

    # fastq
    event = {
             'bucket': '***REMOVED***-dev-from-personalis',
             'componentCount': 32,
             'contentType': 'application/octet-stream',
             'crc32c': 'ftNG8w==',
             'etag': 'CL3nyPj80uECEBE=',
             'generation': '1555361455813565',
             'id': '***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565',
             'kind': 'storage#object',
             'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media',
             'metadata': {'function-testing': '20190423:1217', 'gcf-update-metadata': '510893936442804'},
             'metageneration': '17',
             'name': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz',
             'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz',
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
        expected_query = 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "***REMOVED***-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECEBE=", generation: "1555361455813565", id: "***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "17", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-04-23T19:17:53.205Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556047073.205, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-04-23T19:17:53.205000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, readGroup: 0}) RETURN node'
        assert summary['db-query'] == expected_query
        print("> Query test: Pass.")
    except:
        print("! Error: database query does not match expected.")
        pdb.set_trace()
    # Test pubsub message

    # ubam
    event = {'bucket': '***REMOVED***-dev-from-personalis-gatk', 'contentType': 'application/octet-stream', 'crc32c': 'ZaJM+g==', 'etag': 'CPiFjqbVgOICEAI=', 'generation': '1556931361866488', 'id': '***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/objects/SHIP4946367_0.ubam/1556931361866488', 'kind': 'storage#object', 'md5Hash': 'Tgh+eyIiKe8TRWV6vohGJQ==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Fobjects%2FSHIP4946367_0.ubam?generation=1556931361866488&alt=media', 'metadata': {'test': '20190506:1148'}, 'metageneration': '2', 'name': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/objects/SHIP4946367_0.ubam', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Fobjects%2FSHIP4946367_0.ubam', 'size': '16871102587', 'storageClass': 'REGIONAL', 'timeCreated': '2019-05-04T00:56:01.866Z', 'timeStorageClassUpdated': '2019-05-04T00:56:01.866Z', 'updated': '2019-05-06T18:48:06.711Z'}
    context = None
    summary = create_node_query(event, context)
