import os
import re
import json
import yaml
import importlib

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
    PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT', '')
    TOPIC = parsed_vars.get('DB_QUERY_TOPIC', '')
    DATA_GROUP = parsed_vars.get('DATA_GROUP', '')


    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID,
                                                       topic = TOPIC)


def create_node_query(db_entry, dry_run=False):
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


def get_node_labels(name, label_patterns):
    labels = []
    for key, values in label_patterns.items():
        for pattern in values:
            match = re.fullmatch(pattern, name)
            if match:
                labels.append(key)
                break
    return labels


def gen_new_blob_query(event, context):
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
    label_patterns = node_kinds.get_match_patterns()

    # Determine which label patterns match the blob name
    labels = get_node_labels(name, label_patterns)
    if not labels:
        raise RuntimeError(f"Blob '{name}' does not match any label patterns.")

    # Get label-specific metadata functions
    label_functions = node_kinds.get_label_functions(labels)

    node_obj = node_module.NodeEntry(
                                       event,
                                       context,
                                       labels,
                                       label_functions)
    node_dict = node_obj.get_db_dict()

    print(f"> Generating database query for node: {node_dict}.")
    db_query = create_node_query(node_dict)
    print(f"> Database query: \"{db_query}\".")

    message = {
               "resource": "query",
               "neo4j-metadata": {
                                    "cypher": db_query, 
                                    "result-mode": "data",
               },
               "trellis-metadata": {
                                    "publish-topic": f"{DATA_GROUP}-new-node", 
                                    "result-structure": "list",
                                    "result-split": "True"
               },
    }
    print(f"> Pubsub message: {message}.")

    message = json.dumps(message).encode('utf-8')
    result = PUBLISHER.publish(TOPIC_PATH, data=message).result()
    print(f"> Published query to {TOPIC_PATH}. {result}.")
    return

    # Execute node triggers (DEV)
    # TODO: Test in dev
    trigger_module_name = f"{DATA_GROUP}.{bucket_name}.trigger-config"
    trigger_module = importlib.import_module(trigger_module_name)

    trigger_config = trigger_module.NodeTriggers(
                                                 project_id = PROJECT_ID,
                                                 node = node_dict)
    triggers = trigger_config.get_triggers()
    print(f"> Node triggers: {triggers}.")
    trigger_config.execute_triggers()

    summary = {
               "name": name, 
               "bucket": bucket_name, 
               "node-module-name": node_module_name, 
               "trigger-module-name": trigger_module_name, 
               "labels": labels, 
               "db-query": db_query,
    }
    return(summary)


if __name__ == "__main__":
    # Run unit tests in local
    PROJECT_ID = "***REMOVED***-dev"
    TOPIC = "wgs-35000-db-queries"
    DATA_GROUP = 'wgs-35000'

    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID,
                                                       topic = TOPIC)

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

    summary = gen_new_blob_query(event, context)
    
    # Test event attributes
    assert summary['name'] == event['name']
    assert summary['bucket'] == event['bucket']
    
    # Test node and trigger modules
    assert summary['node-module-name'] == f"wgs-35000.{event['bucket']}.create-node-config"
    assert summary['trigger-module-name'] == f"wgs-35000.{event['bucket']}.trigger-config"
    
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
        expected_query = 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "***REMOVED***-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECEBE=", generation: "1555361455813565", id: "***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "17", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-04-23T19:17:53.205Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556047073.205, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-04-23T19:17:53.205000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, index: 0}) RETURN node'
        assert summary['db-query'] == expected_query
        print("> Query test: Pass.")
    except:
        print("! Error: database query does not match expected.")
    # Test pubsub message