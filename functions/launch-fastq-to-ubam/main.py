import os
import pdb
import sys
import json
import time
import uuid
import yaml
import base64
import random
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
    NEW_JOB_TOPIC = parsed_vars['NEW_JOBS_TOPIC']

    REGIONS = parsed_vars['DSUB_REGIONS']
    OUT_BUCKET = parsed_vars['DSUB_OUT_BUCKET']
    LOG_BUCKET = parsed_vars['DSUB_LOG_BUCKET']
    DSUB_USER = parsed_vars['DSUB_USER']
    NETWORK = parsed_vars['DSUB_NETWORK']
    SUBNETWORK = parsed_vars['DSUB_SUBNETWORK']


    PUBLISHER = pubsub.PublisherClient()

def format_pubsub_message(job_dict, seed_id, event_id):
    message = {
               "header": {
                          "resource": "job-metadata", 
                          "method": "POST",
                          "labels": ["Create", "Job", "Dsub", "Node"],
                          "sentFrom": f"{FUNCTION_NAME}",
                          "seedId": f"{seed_id}",
                          "previousEventId": f"{event_id}"
               },
               "body": {
                        "node": job_dict,
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
        result = dsub.dsub_main('dsub', dsub_args)
    except ValueError as exception:
        logging.error(f'Problem with dsub arguments: {dsub_args}')
        raise
    except:
        print("> Unexpected error:", sys.exc_info())
        raise
        #for arg in dsub_args:
        #    print(arg)
        #return(sys.exc_info())
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


def make_unique_task_id(nodes, datetime_stamp):
    # Create pretty-unique hash value based on input nodes
    # https://www.geeksforgeeks.org/ways-sort-list-dictionaries-values-python-using-lambda-function/
    sorted_nodes = sorted(nodes, key = lambda i: i['id'])
    nodes_str = json.dumps(sorted_nodes, sort_keys=True, ensure_ascii=True, default=str)
    nodes_hash = hashlib.sha256(nodes_str.encode('utf-8')).hexdigest()
    print(nodes_hash)
    trunc_nodes_hash = str(nodes_hash)[:8]
    task_id = f"{datetime_stamp}-{trunc_nodes_hash}"
    return(task_id, trunc_nodes_hash)


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

    # Get seed/event ID to track provenance of Trellis events
    seed_id = header['seedId']
    event_id = context.event_id

    dry_run = header.get('dryRun')
    if not dry_run:
        dry_run = False

    nodes = body['results'].get('nodes')
    if not nodes:
        print("> No nodes provided. Exiting.")
        return

    if len(nodes) != 2:
        raise ValueError(f"> Error: Need 2 fastqs; {len(nodes)} provided.")

    # Create unique task ID
    datetime_stamp = get_datetime_stamp()
    task_id, trunc_nodes_hash = make_unique_task_id(nodes, datetime_stamp)

    # TODO: Implement QC checking to make sure fastqs match
    #set_sizes = []
    fastq_fields = []
    fastqs = {}
    
    # inputIds used to create relationships via trigger
    input_ids = []
    for node in nodes:
        plate = node['plate']
        sample = node['sample']
        read_group = node['readGroup']
        mate_pair = node['matePair']
        #set_size = int(node['setSize'])/2
        input_id = node['id']

        bucket = node['bucket']
        path = node['path']

        fastq_name = f'FASTQ_{mate_pair}'
        fastqs[fastq_name] = f"gs://{bucket}/{path}" 

        fastq_fields.extend([plate, sample, read_group])
        #set_sizes.append(set_size)
        input_ids.append(input_id)

    # Check that fastqs are from same sample/read group
    if len(set(fastq_fields)) != 3:
        raise ValueError(f"> Fastq fields are not in agreement: {fastq_fields}. Exiting.")

    # Check to make sure that set sizes are in agreement
    #if len(set(set_sizes)) != 1:
    #    raise ValueError(f"> Set sizes of fastqs are not in agreement: {set_sizes}. Exiting.")


    # Define logging & outputs after task_id
    task_name = 'fastq-to-ubam'
    unique_task_label = "FastqToUbam"
    job_dict = {
                "provider": "google-cls-v2",
                "user": DSUB_USER,
                "regions": REGIONS,
                "project": PROJECT_ID,
                "minCores": 1,
                "minRam": 7.5,
                "bootDiskSize": 20,
                "image": f"gcr.io/{PROJECT_ID}/broadinstitute/gatk:4.1.0.0",
                "logging": f"gs://{LOG_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/logs",
                "diskSize": 500,
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
                "trellisTaskId": task_id,
                "dryRun": dry_run,
                #"preemptible": "3",
                #"retries": "3",
                "preemptible": False,
                "sample": sample,
                "plate": plate,
                "readGroup": read_group,
                "name": task_name,
                "inputHash": trunc_nodes_hash,
                "labels": ["Job", "Dsub", unique_task_label],
                "inputIds": input_ids,
                "network": NETWORK,
                "subnetwork": SUBNETWORK,
    }

    dsub_args = [
        #"--name", job_dict["name"],
        "--name", f"fq2u-{job_dict['inputHash'][0:5]}",
        "--label", f"read-group={read_group}",
        "--label", f"sample={sample.lower()}",
        "--label", f"trellis-id={task_id}",
        "--label", f"trellis-name={job_dict['name']}",
        "--label", f"plate={plate.lower()}",
        "--label", f"input-hash={trunc_nodes_hash}",
        "--label", f"wdl-call-alias={task_name}",
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
        "--command", job_dict["command"],
        "--use-private-address",
        "--network", job_dict["network"],
        "--subnetwork", job_dict["subnetwork"],
        "--enable-stackdriver-monitoring",
        # 4 total attempts; 3 preemptible, final 1 full-price
        #"--preemptible", job_dict["preemptible"],
        #"--retries", job_dict["retries"] 
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
    if job_dict['dryRun']:
        dsub_args.append("--dry-run")

    # Wait a random time interval to reduce overlapping db queries
    #   because of ubam objects created at same time
    random_wait = random.randrange(0,10)
    print(f"> Waiting for {random_wait} seconds to launch job.")
    time.sleep(random_wait)
    
    # Launch dsub job
    print(f"> Launching dsub with args: {dsub_args}.")
    dsub_result = launch_dsub_task(dsub_args)
    print(f"> Dsub result: {dsub_result}.")

    """ DEPRECATED IN VERSION 1.2.3
    # Metadata to be perpetuated to ubams is written to file
    # Try until success
    metadata = {"setSize": set_size}
    if 'job-id' in dsub_result.keys() and metadata and not dry_run:
        print(f"> Metadata passed to output blobs: {metadata}.")
        # Dump metadata into GCS blob
        meta_blob_path = f"{plate}/{sample}/{task_name}/{task_id}/metadata/all-objects.json"
        while True:
            result = write_metadata_to_blob(meta_blob_path, metadata)
            if result == True:
                break
        print(f"> Created metadata blob at gs://{OUT_BUCKET}/{meta_blob_path}.")
    """
    
    if 'job-id' in dsub_result.keys():
        # Add dsub job ID to neo4j database node
        job_dict['dsubJobId'] = dsub_result['job-id']
        job_dict['dstatCmd'] = (
                                 "dstat " +
                                f"--project {job_dict['project']} " +
                                f"--provider {job_dict['provider']} " +
                                f"--jobs '{job_dict['dsubJobId']}' " +
                                f"--users '{job_dict['user']}' " +
                                 "--full " +
                                 "--format json " +
                                 "--status '*'")

        # Format inputs for neo4j database
        for key, value in job_dict["inputs"].items():
            job_dict[f"input_{key}"] = value
        for key, value in job_dict["envs"].items():
            job_dict[f"env_{key}"] = value
        for key, value in job_dict["outputs"].items():
            job_dict[f"output_{key}"] = value

        # Send job metadata to create-job-node function
        message = format_pubsub_message(
                                        job_dict = job_dict, 
                                        #nodes = nodes,
                                        seed_id = seed_id,
                                        event_id = event_id)
        print(f"> Pubsub message: {message}.")
        result = publish_to_topic(
                                  PUBLISHER,
                                  PROJECT_ID,
                                  NEW_JOB_TOPIC,
                                  message) 
        print(f"> Published message to {NEW_JOB_TOPIC} with result: {result}.")
