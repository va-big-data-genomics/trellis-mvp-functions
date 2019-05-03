import os
import re
import json
import yaml
import base64
import importlib

from google.cloud import storage
from google.cloud import pubsub

# Get runtime variables from cloud storage bucket
# https://www.sethvargo.com/secrets-in-serverless/
ENVIRONMENT = os.environ.get('ENVIRONMENT')
if ENVIRONMENT == 'google-cloud':
    TRIGGER = os.environ['TRIGGER']
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    # Runtime variables
    PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT')
    TOPIC = parsed_vars.get('DB_QUERY_TOPIC')

    PUBLISHER = pubsub.PublisherClient()


def publish_message(topic_path, message):
    message = json.dumps(message).encode('utf-8')
    print(f'> Publishing message "{message}".')
    result = PUBLISHER.publish(topic_path, data=message).result()
    print(f'> Message published to {topic_path}: {result}.')   


def check_triggers(event, context):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    # Trellis config data
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"> Processing pubsub message: {data}.")
    resource = data['resource']
    query = data['query']
    result = data['result']

    if isinstance(result, str):
        result = json.loads(result)

    # Check that resource is query
    if resource != 'query-result':
        print(f"Error: Expected resource type 'request', " +
              f"got '{data['resource']}.'")
        return

    trigger_module_name = f"{TRIGGER}_triggers"
    trigger_module = importlib.import_module(trigger_module_name)

    if TRIGGER == 'node':
        trigger_config = trigger_module.NodeTriggers(
                                                     project_id = PROJECT_ID,
                                                     node = result['node']) #????
    elif TRIGGER == 'property':
        trigger_config = trigger_module.PropertyTriggers(
                                                         project_id = PROJECT_ID,
                                                         properties = result)

    triggers = trigger_config.get_triggers()
    #trigger_config.execute_triggers()
    for trigger in triggers:
        print(f'> Executing trigger: {triggers}.')
        topic_path, message = trigger()
        publish_message(topic_path, message)

    #summary = {
    #           "name": name, 
    #           "bucket": bucket_name, 
    #           "node-module-name": node_module_name, 
    #           "trigger-module-name": trigger_module_name, 
    #           "labels": labels, 
    #           "db-query": db_query,
    #}
    #return(summary)


if __name__ == "__main__":
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"
    TOPIC = "wgs35-db-queries"
    TRIGGER = 'property'

    PUBLISHER = pubsub.PublisherClient()

    # Property test
    data = {
             "resource": "query-result",
             "query": "<SOME QUERY>",
             "result": '{"added_setSize": 16, "nodes_sample": "SHIP4946367", "nodes_labels": ["Fastq", "WGS_35000", "Blob"]}' 

    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context)

    # Node test
    TRIGGER = 'node'
    data = {'resource': 'query-result', 'query': 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "gbsc-gcp-project-mvp-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECECg=", generation: "1555361455813565", id: "gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "40", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-05-03T19:12:55.055Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556910775.055, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-05-03T19:12:55.055000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, readGroup: 0}) RETURN node', 'result': {'node': {'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz', 'storageClass': 'REGIONAL', 'timeCreatedEpoch': 1555361455.813, 'timeUpdatedEpoch': 1556910775.055, 'timeCreated': '2019-04-15T20:50:55.813Z', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565', 'contentType': 'application/octet-stream', 'generation': '1555361455813565', 'metageneration': '40', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T19:12:55.055000+00:00', 'sample': 'SHIP4946367', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R1.fastq.gz', 'crc32c': 'ftNG8w==', 'size': 5955984357, 'timeStorageClassUpdated': '2019-04-15T20:50:55.813Z', 'name': 'SHIP4946367_0_R1', 'etag': 'CL3nyPj80uECECg=', 'timeCreatedIso': '2019-04-15T20:50:55.813000+00:00', 'matePair': 1, 'updated': '2019-05-03T19:12:55.055Z'}}}
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context)

