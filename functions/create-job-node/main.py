import os
import re
import pdb
import json
import pytz
import yaml
import base64
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
    if clean_dict.get('size'):
        clead_dict['size'] = int(clean_dict['size'])

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


def write_job_node_query(event, context):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    #print(f"> Processing new Pub/Sub message: {context['event_id']}.")
    print(f"> Context: {context}.")
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"> Data: {data}.")

    header = data['header']
    body = data['body']

    # Create dict of metadata to add to database node
    db_dict = clean_metadata_dict(body['node'])

    # Add standard fields
    time_fields = get_standard_time_fields(event)
    db_dict.update(time_fields)

    print(f"> Generating database query for node: {db_dict}.")
    db_query = format_query(db_dict)
    print(f"> Database query: \"{db_query}\".")

    message = {
               "header": {
                          "method": "POST", 
                          "labels": ['Job', 'Create', 'Node', 'Query', 'Cypher'], 
                          "resource": "query",
               },
               #"resource": "query",
               "body": {
                    #"neo4j-metadata": {
                                       "cypher": db_query, 
                                       "result-mode": "data",
                    #},
                    #"trellis-metadata": {
                                         "publish-topic": f"{DATA_GROUP}-add-relationships", 
                                         "result-structure": "list",
                                         "result-split": "False",
                    #},
                    "perpetuate": body["perpetuate"],
                }
    }
    
    message = json.dumps(message).encode('utf-8')
    print(f"> Pubsub message: {message}.")
    result = PUBLISHER.publish(TOPIC_PATH, data=message).result()
    print(f"> Published query to {TOPIC_PATH}. {result}.")


if __name__ == "__main__":
    # Run unit tests in local
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"
    TOPIC = "function-test"
    DATA_GROUP = 'wgs35'

    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID,
                                                       topic = TOPIC)

    # gatk-5-dollar job
    """
    data = {
            "header": {
                       "method": "POST", 
                       "labels": ["Job", "Cromwell", "Command", "Args", "Inputs"], 
                       "resource": "job-metadata"
                      }, 
                      "body": {
                               "node": {
                                        "provider": "google-v2", 
                                        "user": "trellis", 
                                        "zones": "us-west1*", 
                                        "project": "gbsc-gcp-project-mvp-dev", 
                                        "min_cores": 1, 
                                        "min_ram": 6.5, 
                                        "preemptible": true, 
                                        "boot_disk_size": 20, 
                                        "image": "gcr.io/gbsc-gcp-project-mvp-dev/jinasong/wdl_runner:latest", 
                                        "logging": "gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk-logs/SHIP4946367/fastq-to-vcf/gatk-5-dollar/logs", 
                                        "disk-size": 1000, 
                                        "command": "java -Dconfig.file=${CFG} -Dbackend.providers.JES.config.project=${MYproject} -Dbackend.providers.JES.config.root=${ROOT} -jar /cromwell/cromwell.jar run ${WDL} --inputs ${INPUT} --options ${OPTION}", 
                                        "inputs": {
                                                   "CFG": "gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf", 
                                                   "OPTION": "gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json", 
                                                   "WDL": "gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl", 
                                                   "SUBWDL": "gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl", 
                                                   "INPUT": "gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json"
                                                  }, 
                                        "envs": {
                                                 "MYproject": "gbsc-gcp-project-mvp-dev", 
                                                 "ROOT": "gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output"
                                                }, 
                                        "dry-run": True, 
                                        "labels": ["Job", "Cromwell"], 
                                        "input_CFG": "gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf", 
                                        "input_OPTION": "gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json", 
                                        "input_WDL": "gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl", 
                                        "input_SUBWDL": "gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl", 
                                        "input_INPUT": "gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json", 
                                        "env_MYproject": "gbsc-gcp-project-mvp-dev", 
                                        "env_ROOT": "gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output"
                               }, 
                               "perpetuate": {
                                              "relationships": {
                                                                "to-node": {
                                                                            "INPUT_TO": [
                                                                                         {"basename": "SHIP4946367_2.ubam", "bucket": "gbsc-gcp-project-mvp-dev-from-personalis-gatk", "contentType": "application/octet-stream", "crc32c": "ojStVg==", "dirname": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output", "etag": "CJTpxe3ynuICEAM=", "extension": "ubam", "generation": "1557970088457364", "id": "gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam/1557970088457364", "kind": "storage#object", "labels": ["WGS35", "Blob", "Ubam"], "md5Hash": "opGAi0f9olAu4DKzvYiayg==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam?generation=1557970088457364&alt=media", "metageneration": "3", "name": "SHIP4946367_2", "path": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam", "sample": "SHIP4946367", "selfLink": "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam", "size": 16886179620, "storageClass": "REGIONAL", "timeCreated": "2019-05-16T01:28:08.455Z", "timeCreatedEpoch": 1557970088.455, "timeCreatedIso": "2019-05-16T01:28:08.455000+00:00", "timeStorageClassUpdated": "2019-05-16T01:28:08.455Z", "timeUpdatedEpoch": 1558045261.522, "timeUpdatedIso": "2019-05-16T22:21:01.522000+00:00", "trellisTask": "fastq-to-ubam", "trellisWorkflow": "fastq-to-vcf", "updated": "2019-05-16T22:21:01.522Z"}, 
                                                                                         {"basename": "SHIP4946367_0.ubam", "bucket": "gbsc-gcp-project-mvp-dev-from-personalis-gatk", "contentType": "application/octet-stream", "crc32c": "ZaJM+g==", "dirname": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output", "etag": "CM+sxKDynuICEAY=", "extension": "ubam", "generation": "1557969926952527", "id": "gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam/1557969926952527", "kind": "storage#object", "labels": ["WGS35", "Blob", "Ubam"], "md5Hash": "Tgh+eyIiKe8TRWV6vohGJQ==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam?generation=1557969926952527&alt=media", "metageneration": "6", "name": "SHIP4946367_0", "path": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam", "sample": "SHIP4946367", "selfLink": "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam", "size": 16871102587, "storageClass": "REGIONAL", "timeCreated": "2019-05-16T01:25:26.952Z", "timeCreatedEpoch": 1557969926.952, "timeCreatedIso": "2019-05-16T01:25:26.952000+00:00", "timeStorageClassUpdated": "2019-05-16T01:25:26.952Z", "timeUpdatedEpoch": 1558045265.901, "timeUpdatedIso": "2019-05-16T22:21:05.901000+00:00", "trellisTask": "fastq-to-ubam", "trellisWorkflow": "fastq-to-vcf", "updated": "2019-05-16T22:21:05.901Z"}
                                                                            ]
                                                                }
                                              }
                               }
                      }
            }
    """
    data = {'header': {'method': 'POST', 'labels': ['Job', 'Cromwell', 'Command', 'Args', 'Inputs'], 'resource': 'job-metadata'}, 'body': {'node': {'provider': 'google-v2', 'user': 'trellis', 'zones': 'us-west1*', 'project': 'gbsc-gcp-project-mvp-dev', 'minCores': 1, 'minRam': 6.5, 'preemptible': True, 'bootDiskSize': 20, 'image': 'gcr.io/gbsc-gcp-project-mvp-dev/jinasong/wdl_runner:latest', 'logging': 'gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk-logs/SHIP4946367/fastq-to-vcf/gatk-5-dollar/logs', 'diskSize': 1000, 'command': 'java -Dconfig.file=${CFG} -Dbackend.providers.JES.config.project=${MYproject} -Dbackend.providers.JES.config.root=${ROOT} -jar /cromwell/cromwell.jar run ${WDL} --inputs ${INPUT} --options ${OPTION}', 'inputs': {'CFG': 'gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf', 'OPTION': 'gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json', 'WDL': 'gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl', 'SUBWDL': 'gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl', 'INPUT': 'gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json'}, 'envs': {'MYproject': 'gbsc-gcp-project-mvp-dev', 'ROOT': 'gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output'}, 'dryRun': True, 'labels': ['Job', 'Cromwell'], 'input_CFG': 'gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf', 'input_OPTION': 'gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json', 'input_WDL': 'gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl', 'input_SUBWDL': 'gs://gbsc-gcp-project-mvp-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl', 'input_INPUT': 'gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json', 'env_MYproject': 'gbsc-gcp-project-mvp-dev', 'env_ROOT': 'gs://gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output'}, 'perpetuate': {'relationships': {'to-node': {'INPUT_TO': [{'basename': 'SHIP4946367_2.ubam', 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis-gatk', 'contentType': 'application/octet-stream', 'crc32c': 'ojStVg==', 'dirname': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output', 'etag': 'CJTpxe3ynuICEAM=', 'extension': 'ubam', 'generation': '1557970088457364', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam/1557970088457364', 'kind': 'storage#object', 'labels': ['WGS35', 'Blob', 'Ubam'], 'md5Hash': 'opGAi0f9olAu4DKzvYiayg==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam?generation=1557970088457364&alt=media', 'metageneration': '3', 'name': 'SHIP4946367_2', 'path': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam', 'size': 16886179620, 'storageClass': 'REGIONAL', 'timeCreated': '2019-05-16T01:28:08.455Z', 'timeCreatedEpoch': 1557970088.455, 'timeCreatedIso': '2019-05-16T01:28:08.455000+00:00', 'timeStorageClassUpdated': '2019-05-16T01:28:08.455Z', 'timeUpdatedEpoch': 1558045261.522, 'timeUpdatedIso': '2019-05-16T22:21:01.522000+00:00', 'trellisTask': 'fastq-to-ubam', 'trellisWorkflow': 'fastq-to-vcf', 'updated': '2019-05-16T22:21:01.522Z'}, {'basename': 'SHIP4946367_0.ubam', 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis-gatk', 'contentType': 'application/octet-stream', 'crc32c': 'ZaJM+g==', 'dirname': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output', 'etag': 'CM+sxKDynuICEAY=', 'extension': 'ubam', 'generation': '1557969926952527', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam/1557969926952527', 'kind': 'storage#object', 'labels': ['WGS35', 'Blob', 'Ubam'], 'md5Hash': 'Tgh+eyIiKe8TRWV6vohGJQ==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam?generation=1557969926952527&alt=media', 'metageneration': '6', 'name': 'SHIP4946367_0', 'path': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam', 'size': 16871102587, 'storageClass': 'REGIONAL', 'timeCreated': '2019-05-16T01:25:26.952Z', 'timeCreatedEpoch': 1557969926.952, 'timeCreatedIso': '2019-05-16T01:25:26.952000+00:00', 'timeStorageClassUpdated': '2019-05-16T01:25:26.952Z', 'timeUpdatedEpoch': 1558045265.901, 'timeUpdatedIso': '2019-05-16T22:21:05.901000+00:00', 'trellisTask': 'fastq-to-ubam', 'trellisWorkflow': 'fastq-to-vcf', 'updated': '2019-05-16T22:21:05.901Z'}]}}}}}
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}   
    context = {'event_id': 'Test'}

    write_job_node_query(event, context)

