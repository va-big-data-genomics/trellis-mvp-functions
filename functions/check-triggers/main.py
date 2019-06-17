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
    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    # Runtime variables
    PROJECT_ID = parsed_vars.get('GOOGLE_CLOUD_PROJECT')
    TOPIC = parsed_vars.get('DB_QUERY_TOPIC')

    PUBLISHER = pubsub.PublisherClient()


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result


#def publish_message(topic_path, message):
#    message = json.dumps(message).encode('utf-8')
#    print(f'> Publishing message "{message}".')
#    result = PUBLISHER.publish(topic_path, data=message).result()
#    print(f'> Message published to {topic_path}: {result}.')   


def check_triggers(event, context):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    # Trellis config data
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"> Received pubsub message: {data}.")
    header = data['header']
    body = data['body']

    resource = header['resource']
    query = body['query']
    results = body['results']

    if isinstance(results, str):
        results = json.loads(results)

    # Check that resource is query
    if resource != 'queryResult':
        print(f"Error: Expected resource type 'queryResult', " +
              f"got '{header['resource']}.'")
        return

    trigger_module_name = f"{TRIGGER}_triggers"
    trigger_module = importlib.import_module(trigger_module_name)

    if TRIGGER == 'node':
        trigger_config = trigger_module.NodeTriggers(
                                                     project_id = PROJECT_ID,
                                                     node = results[0]['node']) #????
    elif TRIGGER == 'property':
        trigger_config = trigger_module.PropertyTriggers(
                                                         project_id = PROJECT_ID,
                                                         properties = results[0])

    triggers = trigger_config.get_triggers()
    if triggers:
        for trigger in triggers:
            print(f'> Executing trigger: {triggers}.')
            topic, message = trigger(FUNCTION_NAME)
            print(f"> Publishing message: {message}.")
            result = publish_to_topic(topic, message)
            print(f"> Published message to {topic} with result: {result}.")
    else:
        print(f'> No triggers executed.')

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
             "results": '{"added_setSize": 16, "nodes_sample": "SHIP4946367", "nodes_labels": ["Fastq", "WGS_35000", "Blob"]}' 

    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context)

    # Fastq node test
    TRIGGER = 'node'
    data = {
            'resource': 'query-result', 
            'query': 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "gbsc-gcp-project-mvp-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECECg=", generation: "1555361455813565", id: "gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "40", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-05-03T19:12:55.055Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556910775.055, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-05-03T19:12:55.055000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, readGroup: 0}) RETURN node', 
            'results': {'node': {'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz', 'storageClass': 'REGIONAL', 'timeCreatedEpoch': 1555361455.813, 'timeUpdatedEpoch': 1556910775.055, 'timeCreated': '2019-04-15T20:50:55.813Z', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565', 'contentType': 'application/octet-stream', 'generation': '1555361455813565', 'metageneration': '40', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T19:12:55.055000+00:00', 'sample': 'SHIP4946367', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R1.fastq.gz', 'crc32c': 'ftNG8w==', 'size': 5955984357, 'timeStorageClassUpdated': '2019-04-15T20:50:55.813Z', 'name': 'SHIP4946367_0_R1', 'etag': 'CL3nyPj80uECECg=', 'timeCreatedIso': '2019-04-15T20:50:55.813000+00:00', 'matePair': 1, 'updated': '2019-05-03T19:12:55.055Z'}}}
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context)

    # Json node test
    TRIGGER = 'node'
    data = {
            'resource': 'query-result', 
            'query': 'CREATE (node:Json:WGS_35000:Blob {bucket: "gbsc-gcp-project-mvp-dev-from-personalis", contentType: "application/json", crc32c: "3fotNQ==", etag: "CKPi8vn80uECEA8=", generation: "1555361458598179", id: "gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/SHIP4946367.json/1555361458598179", kind: "storage#object", md5Hash: "sLK5JVGK7A9Xbcb4suIA8g==", mediaLink: "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FSHIP4946367.json?generation=1555361458598179&alt=media", metageneration: "15", name: "SHIP4946367", selfLink: "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FSHIP4946367.json", size: 686, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:58.597Z", timeStorageClassUpdated: "2019-04-15T20:50:58.597Z", updated: "2019-05-03T19:46:19.685Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/SHIP4946367.json", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367", basename: "SHIP4946367.json", extension: "json", timeCreatedEpoch: 1555361458.597, timeUpdatedEpoch: 1556912779.685, timeCreatedIso: "2019-04-15T20:50:58.597000+00:00", timeUpdatedIso: "2019-05-03T19:46:19.685000+00:00", labels: [\'Json\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367"}) RETURN node', 
            'results': {'node': {'extension': 'json', 'dirname': 'va_mvp_phase2/DVALABP000398/SHIP4946367', 'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/SHIP4946367.json', 'storageClass': 'REGIONAL', 'md5Hash': 'sLK5JVGK7A9Xbcb4suIA8g==', 'timeCreatedEpoch': 1555361458.597, 'timeUpdatedEpoch': 1556912779.685, 'timeCreated': '2019-04-15T20:50:58.597Z', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/SHIP4946367.json/1555361458598179', 'contentType': 'application/json', 'generation': '1555361458598179', 'metageneration': '15', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T19:46:19.685000+00:00', 'sample': 'SHIP4946367', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FSHIP4946367.json?generation=1555361458598179&alt=media', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FSHIP4946367.json', 'labels': ['Json', 'WGS_35000', 'Blob'], 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis', 'basename': 'SHIP4946367.json', 'crc32c': '3fotNQ==', 'size': 686, 'timeStorageClassUpdated': '2019-04-15T20:50:58.597Z', 'name': 'SHIP4946367', 'etag': 'CKPi8vn80uECEA8=', 'timeCreatedIso': '2019-04-15T20:50:58.597000+00:00', 'updated': '2019-05-03T19:46:19.685Z'}}}
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context)
