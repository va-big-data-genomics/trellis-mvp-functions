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
import logging

from google.cloud import storage
from google.cloud import pubsub

from datetime import datetime

from dsub.commands import dsub

class Struct:
    # https://stackoverflow.com/questions/6866600/how-to-parse-read-a-yaml-file-into-a-python-object
    def __init__(self, **entries):
        self.__dict__.update(entries)


ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if not ENVIRONMENT:
    ENVIRONMENT == 'local'

if ENVIRONMENT == 'google-cloud':

    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)
    TRELLIS = Struct(**parsed_vars)

    PUBLISHER = pubsub.PublisherClient()
    CLIENT = storage.Client()

class TrellisMessage:

    def __init__(self, event, context):
        """Parse Trellis messages from Pub/Sub event & context.

        Args:
            event (type):
            context (type):

        Message format:
            - context
                - event_id (required)
            - event
                - header
                    - sentFrom (required)
                    - method (optional)
                    - resource (optional)
                    - labels (optional)
                    - seedId (optional)
                    - previousEventId (optional)
                - body
                    - cypher (optional)
                    - results (optional)
        """
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)
        logging.info(f"> Context: {context}.")
        logging.info(f"> Data: {data}.")
        logging.info(f"> Context: {context}.")
        logging.info(f"> Data: {data}.")
        
        header = data['header']
        body = data['body']

        self.event_id = context.event_id
        self.seed_id = header.get('seedId')
        
        # If no seed specified, assume this is the seed event
        if not self.seed_id:
            self.seed_id = self.event_id

        self.header = data['header']
        self.body = data['body']

        self.results = {}
        if body.get('results'):
            self.results = body.get('results')


def format_pubsub_message(job_dict, seed_id, event_id, function_name, unique_task_label):
    message = {
        "header": {
            "resource": "job-metadata",
            "method": "POST",
            "labels": ["Create", "Job", unique_task_label, "Node"],
            "sentFrom": function_name,
            "seedId": seed_id,
            "previousEventId": event_id
        },
        "body": {
            "node": job_dict,
        }
    }
    return message


def publish_to_topic(publisher, project_id, topic, data):
    topic_path = publisher.topic_path(project_id, topic)
    data = json.dumps(data).encode('utf-8')
    result = publisher.publish(topic_path, data=data)
    return result


def get_datetime_stamp():
    now = datetime.now()
    datestamp = now.strftime("%y%m%d-%H%M%S-%f")[:-3]
    return datestamp


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


def load_json(path):
    with open(path) as fh:
        data = json.load(fh)
    return data


def launch_dsub_task(dsub_args):
    try:
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


def load_local_env():
    class Struct:
        # https://stackoverflow.com/questions/6866600/how-to-parse-read-a-yaml-file-into-a-python-object
        def __init__(self, **entries):
            self.__dict__.update(entries)

    with open('trellis-config.yaml', 'r') as fh:
        trellis_vars = yaml.load(fh, Loader=yaml.Loader)
    trellis = Struct(**trellis_vars)
    return trellis


def launch_cnvnator(event, context, test=False):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """

    # Parse message
    message = TrellisMessage(event, context)
    cram = message.results['cram']
    coverage = message.results['alignmentCoverage']

    # Optional fields
    study = message.results.get('study')
    hospitalized = message.results.get('hospitalized')
    recvdActureCare = message.results.get('recvdActureCare')
    stayedInIcu = message.results.get('stayedInIcu')

    # Check that message includes node metadata
    if not cram:
        logging.error("> No Cram provided. Exiting.")
        return(1)

    # Calculate bin size based on alignment coverage to get 
    # read depth/standard deviation ratio between 4 & 5.
    # This equation calculated by pvembu based on limited
    # sample of MVP genomes.
    bin_size = -1.778 * float(coverage) + 182.39
    bin_size = int(bin_size)

    # Create unique task ID
    datetime_stamp = get_datetime_stamp()
    task_id, trunc_nodes_hash = make_unique_task_id([cram], datetime_stamp)

    # Database entry variables
    plate = cram['plate']
    sample = cram['sample']
    basename = cram['basename']

    study_metadata_path = f"study{study}/hospitalized{hospitalized}/recvdActureCare{recvdActureCare}/stayedInIcu{stayedInIcu}"

    task_name = 'cnvnator'
    unique_task_label = 'Cnvnator'
    job_dict = {
        "provider": "google-cls-v2",
        "user": TRELLIS.DSUB_USER,
        "regions": TRELLIS.DSUB_REGIONS,
        "project": TRELLIS.GOOGLE_CLOUD_PROJECT,
        "machineType": "n1-standard-8",
        "bootDiskSize": 200,
        "image": f"gcr.io/{TRELLIS.GOOGLE_CLOUD_PROJECT}/clinicalgenomics/cnvnator:0.4.1",
        "logging": f"gs://{TRELLIS.DSUB_LOG_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/logs",
        "script": f"gs://{TRELLIS.TRELLIS_BUCKET}/functions/{FUNCTION_NAME}/CNVnator.sh",
        "envs": {
            "SAMPLE_ID": sample,
            # NOTE: Using static bin size
            "BIN_SIZE": 100,
            #"BIN_SIZE": bin_size,
        },
        "inputs": {
            "BAM": f"gs://{cram['bucket']}/{cram['path']}",
            # Trying to resolve an issue using CRAMs(?): https://github.com/DecodeGenetics/graphtyper/issues/57
            "REF_CACHE_SOURCE": "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.ref_cache.tar.gz"
        },
        "inputRecursive": f"DIR=gs://{TRELLIS.GOOGLE_CLOUD_PROJECT}-genomics-public-data/references/GRCh38/unzipped",
        "outputs": {
            "ROOT": f"gs://{TRELLIS.DSUB_OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output/{study_metadata_path}/{sample}.root",
            "CALL_OUT": f"gs://{TRELLIS.DSUB_OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output/{study_metadata_path}/{sample}.out",
            "EVAL_OUT": f"gs://{TRELLIS.DSUB_OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output/{study_metadata_path}/{sample}.txt",
            "CALL_VCF": f"gs://{TRELLIS.DSUB_OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output/{study_metadata_path}/{sample}.vcf",
            "GENOTYPE_OUT": f"gs://{TRELLIS.DSUB_OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output/{study_metadata_path}/{sample}_genotype.out"
        },
        "trellisTaskId": task_id,
        "sample": sample,
        "plate": plate,
        "name": task_name,
        "inputHash": trunc_nodes_hash,
        "labels": ["Job", "Dsub", unique_task_label],
        "inputIds": [cram['id']],
        "network": TRELLIS.DSUB_NETWORK,
        "subnetwork": TRELLIS.DSUB_SUBNETWORK,       
    }

    dsub_args = [
        "--name", f"{task_name}-{job_dict['inputHash'][0:5]}",
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
        "--machine-type", job_dict["machineType"], 
        "--logging", job_dict["logging"],
        "--image", job_dict["image"],
        "--use-private-address",
        "--boot-disk-size", str(job_dict["bootDiskSize"]),
        #"--block-external-network",
        "--ssh",
        "--network", job_dict["network"],
        "--subnetwork", job_dict["subnetwork"],        
        "--script", job_dict["script"],
        "--input-recursive", job_dict["inputRecursive"]
    ]

    # Perform dry-run on local execution
    if test:
        dsub_args.extend(['--ssh', '--keep-alive', '600'])
    #    dsub_args.append('--dry-run')

    # Add dsub list arguments
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
    
    # Launch dsub job
    print(f"> Launching dsub with args: {dsub_args}.")
    dsub_result = launch_dsub_task(dsub_args)
    print(f"> Dsub result: {dsub_result}.")

    # Replicate dsub command
    dsub_cmd_args = dsub_args.copy()
    dsub_cmd_args.insert(0, "dsub")
    for arg in dsub_cmd_args:
        dsub_cmd = " ".join(dsub_cmd_args)

    if 'job-id' in dsub_result.keys():
        # Add dsub job ID to neo4j database node
        job_dict['dsubCmd'] = dsub_cmd
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
        message_to_publish = format_pubsub_message(
                                        job_dict = job_dict,
                                        seed_id = message.seed_id,
                                        event_id = message.event_id,
                                        function_name = FUNCTION_NAME,
                                        unique_task_label = unique_task_label)
        print(f"> Pubsub message: {message_to_publish}.")
        result = publish_to_topic(
                                  publisher = PUBLISHER,
                                  project_id = TRELLIS.GOOGLE_CLOUD_PROJECT,
                                  topic = TRELLIS.NEW_JOBS_TOPIC,
                                  data = message_to_publish) 
        print(f"> Published message to {TRELLIS.NEW_JOBS_TOPIC} with result: {result}.")  


