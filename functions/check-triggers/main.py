import os
import re
import pdb
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

    # Load trigger module
    trigger_module_name = f"{DATA_GROUP}-triggers"
    triggers = importlib.import_module(trigger_module_name)
    ALL_TRIGGERS = triggers.get_triggers(FUNCTION_NAME, parsed_vars)


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result


def check_triggers(event, context, dry_run=False):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    # Trellis config data
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f">>> Received pubsub message: {data}.")
    header = data['header']
    body = data['body']

    resource = header['resource']
    #query = body['query']
    results = body['results']

    # Check that resource is query
    if resource != 'queryResult':
        raise ValueError(
                         f"Error: Expected resource type 'queryResult', " +
                         f"got '{header['resource']}.'")

    node = body['results'].get('node')

    activated_triggers = []
    for trigger in ALL_TRIGGERS:
        #status = trigger.check_conditions(node)
        status = trigger.check_conditions(header, body, node)
        if status == True:
            activated_triggers.append(trigger)
            print(f'>>> Trigger activated: {trigger}.')
            #topic, message = trigger.compose_message(header, body, node)
            messages = trigger.compose_message(header, body, node)
            for message in messages:
                topic = message[0]
                data = message[1]
                print(f">>> Publishing message: {data}.")
                if dry_run:
                    print(f">>> Dry run: Would have published message to {topic}.")
                else:
                    result = publish_to_topic(topic, data)
                    print(f">>> Published message to {topic} with result: {result}.")
    return(activated_triggers)                


if __name__ == "__main__":
    PROJECT_ID = "***REMOVED***-dev"
    TOPIC = "wgs35-db-queries"
    TRIGGER = 'property'
    DATA_GROUP = 'wgs35'

    function_name = f"{DATA_GROUP}-check-triggers"
    parsed_vars = {
                   'DB_QUERY_TOPIC': 'null-db-queries',
                   'TOPIC_FASTQ_TO_UBAM': 'null-task-fastq-to-ubam',
                   'TOPIC_GATK_5_DOLLAR': 'null-gatk-5-dollar',
                   'TOPIC_TRIGGERS': 'null-triggers',
    }

    # Load trigger module
    trigger_module_name = f"{DATA_GROUP}_triggers"
    triggers = importlib.import_module(trigger_module_name)
    ALL_TRIGGERS = triggers.get_triggers(function_name, parsed_vars)

    PUBLISHER = pubsub.PublisherClient()

    # Test 1. TriggerFastqToUbam
    data = {
        "header": {
            "resource": "queryResult",
        },
        "body": {
            "results": {
                "node": {
                    "extension": "fastq.gz",
                    "readGroup": 1,
                    "dirname": "va_mvp_phase2/DVALABP000398/SHIP4946368/FASTQ",
                    "path": "va_mvp_phase2/DVALABP000398/SHIP4946368/FASTQ/SHIP4946368_1_R1.fastq.gz",
                    "storageClass": "REGIONAL",
                    "setSize": 8,
                    "timeCreatedEpoch": 1560796306.133,
                    "timeUpdatedEpoch": 1560796306.133,
                    "timeCreated": "2019-06-17T18:31:46.133Z",
                    "id": "***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946368/FASTQ/SHIP4946368_1_R1.fastq.gz/1560796306133887",
                    "contentType": "application/octet-stream",
                    "generation": "1560796306133887",
                    "metageneration": "1",
                    "kind": "storage#object",
                    "timeUpdatedIso": "2019-06-17T18:31:46.133000+00:00",
                    "sample": "SHIP4946368",
                    "selfLink": "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946368%2FFASTQ%2FSHIP4946368_1_R1.fastq.gz",
                    "mediaLink": "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946368%2FFASTQ%2FSHIP4946368_1_R1.fastq.gz?generation=1560796306133887&alt=media",
                    "labels": [
                    "Fastq",
                    "WGS35",
                    "Blob",
                    "FromPersonalis"
                    ],
                    "bucket": "***REMOVED***-dev-from-personalis",
                    "componentCount": 32,
                    "basename": "SHIP4946368_1_R1.fastq.gz",
                    "crc32c": "GiQklQ==",
                    "size": 6798272932,
                    "timeStorageClassUpdated": "2019-06-17T18:31:46.133Z",
                    "name": "SHIP4946368_1_R1",
                    "etag": "CP+mk6uT8eICEAE=",
                    "timeCreatedIso": "2019-06-17T18:31:46.133000+00:00",
                    "matePair": 1,
                    "updated": "2019-06-17T18:31:46.133Z"
                }
            }
        }
    }

    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context, dry_run=True)
    print(f">>> Expect FastqToUbam: {result}.")

    pdb.set_trace()

    # Test 2. Should not trigger anything
    data = {
        'header': {
            'resource': 'queryResult', 
        },
        'body': {
            'results': {
                'node': {
                    'extension': 'fastq.gz', 
                    'readGroup': 0, 
                    'dirname': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ', 
                    'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz', 
                    'storageClass': 'REGIONAL', 
                    'timeCreatedEpoch': 1555361455.813, 
                    'timeUpdatedEpoch': 1556910775.055, 
                    'timeCreated': '2019-04-15T20:50:55.813Z', 
                    'id': '***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565', 
                    'contentType': 'application/octet-stream', 
                    'generation': '1555361455813565', 
                    'metageneration': '40', 
                    'kind': 'storage#object', 
                    'timeUpdatedIso': '2019-05-03T19:12:55.055000+00:00', 
                    'sample': 'SHIP4946367', 
                    'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media', 
                    'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz', 
                    'labels': ['Fastq', 'WGS35', 'Blob'], 
                    'bucket': '***REMOVED***-dev-from-personalis', 
                    'componentCount': 32, 
                    'basename': 'SHIP4946367_0_R1.fastq.gz', 
                    'crc32c': 'ftNG8w==', 
                    'size': 5955984357, 
                    'timeStorageClassUpdated': '2019-04-15T20:50:55.813Z', 
                    'name': 'SHIP4946367_0_R1', 
                    'etag': 'CL3nyPj80uECECg=', 
                    'timeCreatedIso': '2019-04-15T20:50:55.813000+00:00', 
                    'matePair': 1, 
                    'updated': '2019-05-03T19:12:55.055Z'
                }
            }
        }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context, dry_run=True)
    print(f">>> Expect no triggers: {result}.")

    pdb.set_trace()

    # Test 3. Trigger AddFastqSetSize
    data = {
        'header': {
            'resource': 'queryResult'
        },
        'body': {
            'query': 'CREATE (node:Json:WGS_35000:Blob {bucket: "***REMOVED***-dev-from-personalis", contentType: "application/json", crc32c: "3fotNQ==", etag: "CKPi8vn80uECEA8=", generation: "1555361458598179", id: "***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/SHIP4946367.json/1555361458598179", kind: "storage#object", md5Hash: "sLK5JVGK7A9Xbcb4suIA8g==", mediaLink: "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FSHIP4946367.json?generation=1555361458598179&alt=media", metageneration: "15", name: "SHIP4946367", selfLink: "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FSHIP4946367.json", size: 686, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:58.597Z", timeStorageClassUpdated: "2019-04-15T20:50:58.597Z", updated: "2019-05-03T19:46:19.685Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/SHIP4946367.json", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367", basename: "SHIP4946367.json", extension: "json", timeCreatedEpoch: 1555361458.597, timeUpdatedEpoch: 1556912779.685, timeCreatedIso: "2019-04-15T20:50:58.597000+00:00", timeUpdatedIso: "2019-05-03T19:46:19.685000+00:00", labels: [\'Json\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367"}) RETURN node', 
            'results': {
                'node': {
                    'extension': 'json', 
                    'dirname': 'va_mvp_phase2/DVALABP000398/SHIP4946367', 
                    'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/SHIP4946367.json', 
                    'storageClass': 'REGIONAL', 
                    'md5Hash': 'sLK5JVGK7A9Xbcb4suIA8g==', 
                    'timeCreatedEpoch': 1555361458.597, 
                    'timeUpdatedEpoch': 1556912779.685, 
                    'timeCreated': '2019-04-15T20:50:58.597Z', 
                    'id': '***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/SHIP4946367.json/1555361458598179', 
                    'contentType': 'application/json', 
                    'generation': '1555361458598179', 
                    'metageneration': '15', 
                    'kind': 'storage#object', 
                    'timeUpdatedIso': '2019-05-03T19:46:19.685000+00:00', 
                    'sample': 'SHIP4946367', 
                    'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FSHIP4946367.json?generation=1555361458598179&alt=media', 
                    'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FSHIP4946367.json', 
                    'labels': ['Json', 'WGS35', 'Blob', 'Marker', 'FromPersonalis'], 
                    'bucket': '***REMOVED***-dev-from-personalis', 
                    'basename': 'SHIP4946367.json', 
                    'crc32c': '3fotNQ==', 
                    'size': 686, 
                    'timeStorageClassUpdated': '2019-04-15T20:50:58.597Z', 
                    'name': 'SHIP4946367', 
                    'etag': 'CKPi8vn80uECEA8=', 
                    'timeCreatedIso': '2019-04-15T20:50:58.597000+00:00', 
                    'updated': '2019-05-03T19:46:19.685Z'
                }   
            }
        }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context, dry_run=True)
    print(f">>> Expect AddFastqSetSize: {result}.")

    pdb.set_trace()

    # Test 4. CheckUbamCount
    data = {
        'header': {
            'resource': 'queryResult'
        },
        'body': {
            'results': {
                'node': {
                    "extension": "ubam",
                    "readGroup": 1,
                    "dirname": "SHIP4946367/fastq-to-ubam/0x99cf771c0230-190617-071430/output",
                    "path": "SHIP4946367/fastq-to-ubam/0x99cf771c0230-190617-071430/output/SHIP4946367_1.ubam",
                    "storageClass": "REGIONAL",
                    "setSize": 4,
                    "md5Hash": "GeOsU3LFOFOrE+xQQhJzEA==",
                    "timeCreatedEpoch": 1560759054.243,
                    "timeUpdatedEpoch": 1560792729.531,
                    "timeCreated": "2019-06-17T08:10:54.243Z",
                    "id": "***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-ubam/0x99cf771c0230-190617-071430/output/SHIP4946367_1.ubam/1560759054244092",
                    "contentType": "application/octet-stream",
                    "generation": "1560759054244092",
                    "metageneration": "4",
                    "kind": "storage#object",
                    "timeUpdatedIso": "2019-06-17T17:32:09.531000+00:00",
                    "trellisTask": "fastq-to-ubam",
                    "sample": "SHIP4946367",
                    "mediaLink": "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-ubam%2F0x99cf771c0230-190617-071430%2Foutput%2FSHIP4946367_1.ubam?generation=1560759054244092&alt=media",
                    "selfLink": "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-ubam%2F0x99cf771c0230-190617-071430%2Foutput%2FSHIP4946367_1.ubam",
                    "labels": [ "WGS35","Blob","Ubam"],
                    "bucket": "***REMOVED***-dev-from-personalis-gatk",
                    "basename": "SHIP4946367_1.ubam",
                    "crc32c": "3T8iNw==",
                    "size": 17013633444,
                    "timeStorageClassUpdated": "2019-06-17T08:10:54.243Z",
                    "name": "SHIP4946367_1",
                    "etag": "CPyxiMiI8OICEAQ=",
                    "timeCreatedIso": "2019-06-17T08:10:54.243000+00:00",
                    "updated": "2019-06-17T17:32:09.531Z",
                    "taskId": "0x99cf771c0230-190617-071430"
                }        
            }
        }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    context = None
    result = check_triggers(event, context, dry_run=True)
    print(f">>> Expect CheckUbamCount: {result}.")
