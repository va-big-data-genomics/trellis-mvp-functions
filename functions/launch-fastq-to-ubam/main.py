import os
import sys
import json
import uuid
import yaml
import base64

from google.cloud import storage
from google.cloud import pubsub

from datetime import datetime

from dsub.commands import dsub

ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']

    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    ZONES = parsed_vars['DSUB_ZONES']
    OUT_BUCKET = parsed_vars['DSUB_OUT_BUCKET']
    LOG_BUCKET = parsed_vars['DSUB_LOG_BUCKET']
    DSUB_USER = parsed_vars['DSUB_USER']
    TOPIC = parsed_vars['NEW_JOBS_TOPIC']

    PUBLISHER = pubsub.PublisherClient()

def format_pubsub_message(job_dict, nodes):
    message = {
               "header": {
                          "resource": "job-metadata", 
                          "method": "POST",
                          "labels": ["Job", "Dsub", "Command", "Args", "Inputs"],
                          "sentFrom": f"{FUNCTION_NAME}",
               },
               "body": {
                        "node": job_dict,
                        "perpetuate": {
                            "relationships": {
                                "to-node": {
                                    "INPUT_TO": nodes
                                }
                            }
                        }
               }
    }
    return message


def publish_to_topic(publisher, project_id, topic, data):
    topic_path = publisher.topic_path(project_id, topic)
    message = json.dumps(data).encode('utf-8')
    result = publisher.publish(topic_path, data=message).result()
    return result


def launch_dsub_task(dsub_args):
    try:
        dsub.main('dsub', dsub_args)
    except ValueError as exception:
        print(exception)
        print(f'Error with dsub arguments: {dsub_args}')
        return(exception)
    except:
        print("Unexpected error:", sys.exc_info())
        for arg in dsub_args:
            print(arg)
        return(sys.exc_info())
    return(1)


def get_datetime_stamp():
    now = datetime.now()
    datestamp = now.strftime("%y%m%d-%H%M%S")
    return datestamp

def launch_fastq_to_ubam(event, context):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"> Context: {context}.")
    print(f"> Data: {data}.")
    header = data['header']
    body = data['body']

    dry_run = header.get('dryRun')
    if not dry_run:
        dry_run = False

    # What does this do? 
    #   This is supposed to check that setSize matches...
    #   but it doesn't.
    #metadata = {}
    nodes = body['results']['nodes']
    #for result_name in body['results']:
    #    elements = result_name.split('_')
    #    if elements[0] == 'metadata':
    #        key = elements[1]
    #        metadata[key] = data['results'][result_name]

    if len(nodes) != 2:
        print(f"Error: Need 2 fastqs; {len(nodes)} provided.")
        return

    # Dsub data
    task_name = 'fastq-to-ubam'
    # Create unique task ID
    datetime_stamp = get_datetime_stamp()
    mac_address = hex(uuid.getnode())
    task_id = f"{mac_address}-{datetime_stamp}"

    # TODO: Implement QC checking to make sure fastqs match
    fastqs = {}
    for node in nodes:
        sample = node['sample']
        read_group = node['readGroup']
        mate_pair = node['matePair']

        bucket = node['bucket']
        path = node['path']

        fastq_name = f'FASTQ_{mate_pair}'
        fastqs[fastq_name] = f"gs://{bucket}/{path}" 

    job_dict = {
                "provider": "google-v2",
                "user": DSUB_USER,
                "zones": ZONES,
                "project": PROJECT_ID,
                "minCores": 1,
                "minRam": 7.5,
                "preemptible": True,
                "bootDiskSize": 20,
                "image": f"gcr.io/{PROJECT_ID}/broadinstitute/gatk:4.1.0.0",
                "logging": f"gs://{LOG_BUCKET}/{sample}/{task_name}/{task_id}/logs",
                "diskSize": 1000,
                "command": (
                            '/gatk/gatk ' +
                            '--java-options ' +
                            '\\"-Xmx8G -Djava.io.tmpdir=bla\\" ' +
                            'FastqToSam ' +
                            '-F1 ${FASTQ_1} ' +
                            '-F2 ${FASTQ_2} ' +
                            '-O ${UBAM} ' +
                            '-RG ${RG} ' +
                            '-SM ${SM} ' +
                            '-PL ${PL}'),
                "envs": {
                         "RG": read_group,
                         "SM": sample,
                         "PL": "illumina"
                },
                "inputs": fastqs,
                "outputs": {
                            "UBAM": f"gs://{OUT_BUCKET}/{sample}/{task_name}/{task_id}/output/{sample}_{read_group}.ubam"
                },
                "taskId": task_id,
                "dryRun": dry_run,
    }

    dsub_args = [
        "--provider", job_dict["provider"], 
        "--user", job_dict["user"], 
        "--zones", job_dict["zones"], 
        "--project", job_dict["project"],
        "--min-cores", str(job_dict["minCores"]), 
        "--min-ram", str(job_dict["minRam"]),
        "--boot-disk-size", str(job_dict["bootDiskSize"]), 
        "--image", job_dict["image"], 
        "--logging", job_dict["logging"],
        "--disk-size", str(job_dict["diskSize"]),
        "--command", job_dict["command"]
    ]

    # Argument lists
    for key, value in job_dict["inputs"].items():
        dsub_args.extend([
                          "--input", 
                          f"{key}={value}"])
    for key, value in job_dict['envs'].items():
        dsub_args.extend([
                          "--env",
                          f"{key}={value}"])
    for key, value in job_dict['outputs'].items():
        dsub_args.extend([
                          "--output",
                          f"{key}={value}"])

    # Optional flags
    if job_dict['preemptible']:
        dsub_args.append("--preemptible")
    if job_dict['dryRun']:
        dsub_args.append("--dry-run")

    print(f"Launching dsub with args: {dsub_args}.")
    result = launch_dsub_task(dsub_args)
    print(f"Dsub result: '{result}'.")

    if result == 1:
        job_dict['labels'] = ["Job", "Dsub", "FastqToUbam"]

        for key, value in job_dict["inputs"].items():
            job_dict[f"input_{key}"] = value
        for key, value in job_dict["envs"].items():
            job_dict[f"env_{key}"] = value
        for key, value in job_dict["outputs"].items():
            job_dict[f"output_{key}"] = value

        message = format_pubsub_message(job_dict, nodes)
        print(f"> Pubsub message: {message}.")
        result = publish_to_topic(
                                  PUBLISHER,
                                  PROJECT_ID,
                                  TOPIC,
                                  message) 
        print(f"> Published message to {TOPIC} with result: {result}.")

# For local testing
if __name__ == "__main__":
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"
    ZONES =  "us-west1*"
    OUT_BUCKET = "gbsc-gcp-project-mvp-dev-from-personalis-gatk"
    LOG_BUCKET = "gbsc-gcp-project-mvp-dev-from-personalis-gatk-logs"
    DSUB_USER = "trellis"
    FUNCTION_NAME = "test-launch-fastq-to-ubam"
    TOPIC = "wgs35-create-job-node"

    PUBLISHER = pubsub.PublisherClient()

    data = {
            'header': {
                       'method': 'VIEW',
                       'labels': ['Fastq', 'Nodes'],
                       'resource': 'query-result',
                       'sentFrom': 'wgs35-db-query',
                       'dryRun': False,
            },
            'body': {
                     'query': 'MATCH (n:Fastq) WHERE n.sample="SHIP4946367" WITH n.readGroup AS read_group, collect(n) AS nodes WHERE size(nodes) = 2 RETURN [n in nodes] AS nodes', 
                     'results': {
                                 'nodes': [
                                           {'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz', 'storageClass': 'REGIONAL', 'setSize': 9, 'timeCreatedEpoch': 1555361455.813, 'timeUpdatedEpoch': 1556919952.482, 'timeCreated': '2019-04-15T20:50:55.813Z', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565', 'contentType': 'application/octet-stream', 'generation': '1555361455813565', 'metageneration': '46', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T21:45:52.482000+00:00', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media', 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R1.fastq.gz', 'crc32c': 'ftNG8w==', 'size': 5955984357, 'timeStorageClassUpdated': '2019-04-15T20:50:55.813Z', 'name': 'SHIP4946367_0_R1', 'etag': 'CL3nyPj80uECEC4=', 'timeCreatedIso': '2019-04-15T20:50:55.813000+00:00', 'matePair': 1, 'updated': '2019-05-03T21:45:52.482Z'}, 
                                           {'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz', 'storageClass': 'REGIONAL', 'setSize': 9, 'timeCreatedEpoch': 1555361456.112, 'timeUpdatedEpoch': 1556920219.608, 'timeCreated': '2019-04-15T20:50:56.112Z', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz/1555361456112810', 'contentType': 'application/octet-stream', 'generation': '1555361456112810', 'metageneration': '16', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T21:50:19.608000+00:00', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz?generation=1555361456112810&alt=media', 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R2.fastq.gz', 'crc32c': 'aV17ew==', 'size': 6141826914, 'timeStorageClassUpdated': '2019-04-15T20:50:56.112Z', 'name': 'SHIP4946367_0_R2', 'etag': 'CKqJ2/j80uECEBA=', 'timeCreatedIso': '2019-04-15T20:50:56.112000+00:00', 'matePair': 2, 'updated': '2019-05-03T21:50:19.608Z'}
                                 ], 
                                 'metadata_setSize': 9
                     }
            }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}

    result = launch_fastq_to_ubam(event, context=None)