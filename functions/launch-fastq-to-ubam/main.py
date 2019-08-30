import os
import pdb
import sys
import json
import uuid
import yaml
import base64
import hashlib

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
    REGIONS = parsed_vars['DSUB_REGIONS']
    OUT_BUCKET = parsed_vars['DSUB_OUT_BUCKET']
    LOG_BUCKET = parsed_vars['DSUB_LOG_BUCKET']
    DSUB_USER = parsed_vars['DSUB_USER']
    NEW_JOB_TOPIC = parsed_vars['NEW_JOBS_TOPIC']

    PUBLISHER = pubsub.PublisherClient()

def format_pubsub_message(job_dict, nodes):
    message = {
               "header": {
                          "resource": "job-metadata", 
                          "method": "POST",
                          "labels": ["Create", "Job", "Dsub", "Node"],
                          "sentFrom": f"{FUNCTION_NAME}",
               },
               "body": {
                        "node": job_dict,
               }
    }
    return message


def format_relationship_message(start, end, name, bidirectional):
    message = {
        "header": {
            "resource": "relationship",
            "method": "POST",
            "labels": ["Job", "Create", "Relationship", "Input"],
            "sentFrom": f"{FUNCTION_NAME}"
        },
        "body": {
            "startNode": start,
            "endNode": end,
            "name": name,
            "bidirectional": bidirectional
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
        #dsub.commands.dsub.main('dsub', dsub_args)
        #dsub.main('dsub', dsub_args)
        result = dsub.dsub_main('dsub', dsub_args)
    except ValueError as exception:
        print(exception)
        print(f'Error with dsub arguments: {dsub_args}')
        return(exception)
    except:
        print("Unexpected error:", sys.exc_info())
        for arg in dsub_args:
            print(arg)
        return(sys.exc_info())
    return(result)


def get_datetime_stamp():
    now = datetime.now()
    datestamp = now.strftime("%y%m%d-%H%M%S-%f")[:-3]
    return datestamp


def write_metadata_to_blob(meta_blob_path, metadata):
    try:
        meta_blob = storage.Client(project=PROJECT_ID) \
            .get_bucket(OUT_BUCKET) \
            .blob(meta_blob_path) \
            .upload_from_string(json.dumps(metadata))
        return True
    except:
        return False


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

    nodes = body['results'].get('nodes')
    if not nodes:
        print("> No nodes provided. Exiting.")
        return

    if len(nodes) != 2:
        raise ValueError(f"> Error: Need 2 fastqs; {len(nodes)} provided.")

    # Dsub data
    task_name = 'fastq-to-ubam'
    # Create unique task ID
    datetime_stamp = get_datetime_stamp()

    # https://www.geeksforgeeks.org/ways-sort-list-dictionaries-values-python-using-lambda-function/
    sorted_nodes = sorted(nodes, key = lambda i: i['id'])
    nodes_str = json.dumps(sorted_nodes, sort_keys=True, ensure_ascii=True, default=str)
    nodes_hash = hashlib.sha256(nodes_str.encode('utf-8')).hexdigest()
    print(nodes_hash)
    trunc_nodes_hash = str(nodes_hash)[:8]
    task_id = f"{datetime_stamp}-{trunc_nodes_hash}"

    #pdb.set_trace()

    # TODO: Implement QC checking to make sure fastqs match
    set_sizes = []
    fastq_fields = []
    fastqs = {}
    # inputIds used to create relationships via trigger
    input_ids = []
    for node in nodes:
        plate = node['plate']
        sample = node['sample']
        read_group = node['readGroup']
        mate_pair = node['matePair']
        set_size = int(node['setSize'])/2
        input_id = node['id']

        bucket = node['bucket']
        path = node['path']

        fastq_name = f'FASTQ_{mate_pair}'
        fastqs[fastq_name] = f"gs://{bucket}/{path}" 

        fastq_fields.extend([plate, sample, read_group])
        set_sizes.append(set_size)
        input_ids.append(input_id)

    # Check that fastqs are from same sample/read group
    if len(set(fastq_fields)) != 3:
        raise ValueError(f"> Fastq fields are not in agreement: {fastq_fields}. Exiting.")

    # Check to make sure that set sizes are in agreement
    if len(set(set_sizes)) != 1:
        raise ValueError(f"> Set sizes of fastqs are not in agreement: {set_sizes}. Exiting.")


    # Define logging & outputs after task_id
    job_dict = {
                "provider": "google-v2",
                "user": DSUB_USER,
                "regions": REGIONS,
                "project": PROJECT_ID,
                "minCores": 1,
                "minRam": 7.5,
                "bootDiskSize": 20,
                "image": f"gcr.io/{PROJECT_ID}/broadinstitute/gatk:4.1.0.0",
                "logging": f"gs://{LOG_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/logs",
                "diskSize": 400,
                "command": (
                            '/gatk/gatk ' +
                            '--java-options ' +
                            '\'-Xmx8G -Djava.io.tmpdir=bla\' ' +
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
                            "UBAM": f"gs://{OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output/{sample}_{read_group}.ubam"
                },
                "taskId": task_id,
                "dryRun": dry_run,
                "preemptible": False,
                "sample": sample,
                "plate": plate,
                "readGroup": read_group,
                "name": task_name,
                "inputHash": trunc_nodes_hash,
                "labels": ["Job", "Dsub"],
                "inputIds": input_ids,
    }

    dsub_args = [
        "--name", job_dict["name"],
        "--label", f"read-group={read_group}",
        "--label", f"sample={sample.lower()}",
        "--label", f"trellis-id={task_id}",
        "--label", f"plate={plate.lower()}",
        "--label", f"input-hash={trunc_nodes_hash}",
        "--provider", job_dict["provider"], 
        "--user", job_dict["user"], 
        "--regions", job_dict["regions"],
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
    dsub_result = launch_dsub_task(dsub_args)
    print(f"Dsub result: {dsub_result}.")

    # Metadata to be perpetuated to ubams is written to file
    # Try until success
    metadata = {"setSize": set_size}
    if 'job-id' in dsub_result.keys() and metadata and not dry_run:
        print(f"Metadata passed to output blobs: {metadata}.")
        # Dump metadata into GCS blob
        meta_blob_path = f"{plate}/{sample}/{task_name}/{task_id}/metadata/all-objects.json"
        while True:
            result = write_metadata_to_blob(meta_blob_path, metadata)
            if result == True:
                break
        print(f"Created metadata blob at gs://{OUT_BUCKET}/{meta_blob_path}.")

    
    if 'job-id' in dsub_result.keys():
        # Add dsub job ID to neo4j database node
        job_dict['dsubJobId'] = dsub_result['job-id']
        job_dict['dstatCmd'] = (
                                 "dstat " +
                                f"--provider {job_dict['provider']} " +
                                f"--jobs '{job_dict['job-id']}' " +
                                f"--users '{job_dict['user']}' " +
                                 "--status '*'")

        # Format inputs for neo4j database
        for key, value in job_dict["inputs"].items():
            job_dict[f"input_{key}"] = value
        for key, value in job_dict["envs"].items():
            job_dict[f"env_{key}"] = value
        for key, value in job_dict["outputs"].items():
            job_dict[f"output_{key}"] = value

        # Send job metadata to create-job-node function
        message = format_pubsub_message(job_dict, nodes)
        print(f"> Pubsub message: {message}.")
        result = publish_to_topic(
                                  PUBLISHER,
                                  PROJECT_ID,
                                  NEW_JOB_TOPIC,
                                  message) 
        print(f"> Published message to {NEW_JOB_TOPIC} with result: {result}.")
        

# For local testing
if __name__ == "__main__":
    PROJECT_ID = "***REMOVED***-dev"
    ZONES =  "us-west1*"
    OUT_BUCKET = "***REMOVED***-dev-from-personalis-wgs35"
    LOG_BUCKET = "***REMOVED***-dev-from-personalis-wgs35-logs"
    DSUB_USER = "trellis"
    FUNCTION_NAME = "test-launch-fastq-to-ubam"
    TOPIC = "wgs35-create-job-node"
    REGIONS = "us-west1"

    PUBLISHER = pubsub.PublisherClient()

    data = {
            'header': {
                       'method': 'VIEW',
                       'labels': ['Fastq', 'Nodes'],
                       'resource': 'query-result',
                       'sentFrom': 'wgs35-db-query',
                       'dryRun': True,
            },
            'body': {
                     'query': 'MATCH (n:Fastq) WHERE n.sample="SHIP4946367" WITH n.readGroup AS read_group, collect(n) AS nodes WHERE size(nodes) = 2 RETURN [n in nodes] AS nodes', 
                     'results': {
                                 'nodes': [
                                           {'plate':'DVALBP', 'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz', 'storageClass': 'REGIONAL', 'setSize': 9, 'timeCreatedEpoch': 1555361455.813, 'timeUpdatedEpoch': 1556919952.482, 'timeCreated': '2019-04-15T20:50:55.813Z', 'id': '***REMOVED***-dev-from-personalis/va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565', 'contentType': 'application/octet-stream', 'generation': '1555361455813565', 'metageneration': '46', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T21:45:52.482000+00:00', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media', 'bucket': '***REMOVED***-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R1.fastq.gz', 'crc32c': 'ftNG8w==', 'size': 5955984357, 'timeStorageClassUpdated': '2019-04-15T20:50:55.813Z', 'name': 'SHIP4946367_0_R1', 'etag': 'CL3nyPj80uECEC4=', 'timeCreatedIso': '2019-04-15T20:50:55.813000+00:00', 'matePair': 1, 'updated': '2019-05-03T21:45:52.482Z'}, 
                                           {'plate':'DVALBP', 'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz', 'storageClass': 'REGIONAL', 'setSize': 9, 'timeCreatedEpoch': 1555361456.112, 'timeUpdatedEpoch': 1556920219.608, 'timeCreated': '2019-04-15T20:50:56.112Z', 'id': '***REMOVED***-dev-from-personalis/va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz/1555361456112810', 'contentType': 'application/octet-stream', 'generation': '1555361456112810', 'metageneration': '16', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T21:50:19.608000+00:00', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz?generation=1555361456112810&alt=media', 'bucket': '***REMOVED***-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R2.fastq.gz', 'crc32c': 'aV17ew==', 'size': 6141826914, 'timeStorageClassUpdated': '2019-04-15T20:50:56.112Z', 'name': 'SHIP4946367_0_R2', 'etag': 'CKqJ2/j80uECEBA=', 'timeCreatedIso': '2019-04-15T20:50:56.112000+00:00', 'matePair': 2, 'updated': '2019-05-03T21:50:19.608Z'}
                                 ], 
                                 'metadata_setSize': 9
                     }
            }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}

    result = launch_fastq_to_ubam(event, context=None)