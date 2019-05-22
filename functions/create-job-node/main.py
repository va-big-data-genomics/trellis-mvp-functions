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
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID,
                                                       topic = TOPIC)


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


def get_standard_time_fields(event):
    """
    Args:
        event (dict): Metadata properties stored as strings
    Return
        (dict): Times in iso (str) and from-epoch (int) formats
    """
    datetime_created = datetime.now(pytz.UTC)

    time_created_epoch = get_seconds_from_epoch(datetime_created)
    time_created_iso = datetime_created.isoformat()

    time_fields = {
                   'timeCreatedEpoch': time_created_epoch,
                   'timeCreatedIso': time_created_iso,
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


def format_query(db_entry, dry_run=False):
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
             f"CREATE (node:{labels_str} " +
              "{" + f"{entry_string}" +"}) " +
              "RETURN node")
    return query


def create_node_query(event, context):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    print(f"> Processing new Pub/Sub message: {context['event_id']}.")
    print(f"> Context: {context}.")
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"> Data: {data}.")

    # Create dict of metadata to add to database node
    db_dict = clean_metadata_dict(data['node'])

    # Add standard fields
    time_fields = get_standard_time_fields(event)
    db_dict.update(time_fields)

    print(f"> Generating database query for node: {db_dict}.")
    db_query = format_query(db_dict)
    print(f"> Database query: \"{db_query}\".")

    message = {
               "resource": "query",
               "neo4j-metadata": {
                                    "cypher": db_query, 
                                    "result-mode": "data",
               },
               "trellis-metadata": {
                                    "publish-topic": f"{DATA_GROUP}-add-relationships", 
                                    "result-structure": "list",
                                    "result-split": "False"
               },
               "perpetuate": {"relationships": data["relationships"]}
    }
    print(f"> Pubsub message: {message}.")

    message = json.dumps(message).encode('utf-8')
    result = PUBLISHER.publish(TOPIC_PATH, data=message).result()
    print(f"> Published query to {TOPIC_PATH}. {result}.")


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

