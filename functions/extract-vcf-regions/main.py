import os
import sys
import json
import uuid
import yaml
import base64
import hashlib
import logging

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
    NETWORK = parsed_vars['DSUB_NETWORK']
    SUBNETWORK = parsed_vars['DSUB_SUBNETWORK']

    TRELLIS_BUCKET = parsed_vars['TRELLIS_BUCKET']
    NEW_JOB_TOPIC = parsed_vars['NEW_JOBS_TOPIC']

    # Establish PubSub connection
    PUBLISHER = pubsub.PublisherClient()

def format_pubsub_message(job_dict, seed_id, event_id):
    message = {
        "header": {
            "resource": "job-metadata",
            "method": "POST",
            "labels": ["Create", "Job", "Vcfstats", "Dsub", "Node"],
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

def make_unique_task_id(nodes, datetime_stamp):
    print(nodes, datetime_stamp)
    # Create pretty-unique hash value based on input nodes
    # https://www.geeksforgeeks.org/ways-sort-list-dictionaries-values-python-using-lambda-function/
    sorted_nodes = sorted(nodes, key = lambda i: i['id'])
    nodes_str = json.dumps(sorted_nodes, sort_keys=True, ensure_ascii=True, default=str)
    nodes_hash = hashlib.sha256(nodes_str.encode('utf-8')).hexdigest()
    print(nodes_hash)
    trunc_nodes_hash = str(nodes_hash)[:8]
    task_id = f"{datetime_stamp}-{trunc_nodes_hash}"
    return(task_id, trunc_nodes_hash)

def main(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    
    Extract regions from a VCF that are specified in a BED file.

    Args:
        event (dict): 'data' contains a string describing with the
                       bucket and name of a GCS blob in the format 
                       of : {bucket}#{name}
        context (google.cloud.functions.Context): Metadata for the event.

    Results structure:
        vcf (node): Vcf node to extract regions from
        bed (str): GCS URI pointing to BED file (e.g. 'gs://path')
        fasta_ref (str): GCS URI pointing to Fasta reference file
        fasta_index (str): GCS URI pointng to Fasta index file
        regions_label (str): A name that defines the regions & will be
                             used to label output.
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

    # Used for local testing
    if header.get('dry-run') == 'True':
        dry_run = True
    else:
        dry_run = False

    vcf = body['results'].get('vcf')
    if not vcf:
        print("> No node provided. Exiting.")
        return

    tbi           = body['results']['tbi']
    bed           = body['results']['bed']
    fasta_ref     = body['results']['fasta_ref']
    fasta_index   = body['results']['fasta_index']
    regions_label = body['results']['regions_label']

    if not 'Vcf' in vcf['labels']:
        logging.error(f"Not a VCF object. Ignoring node: {vcf}.")
        return 

    # Create unique task ID
    datetime_stamp = get_datetime_stamp()
    task_id, trunc_nodes_hash = make_unique_task_id([vcf], datetime_stamp)

    # Database entry variables
    bucket = vcf['bucket']
    plate = vcf['plate']
    path = vcf['path']
    sample = vcf['sample']
    basename = vcf['basename']

    task_name = 'extract-vcf-regions'
    unique_task_label = 'ExtractVcfRegions'
    job_dict = {
        "provider": "google-v2",
        "user": DSUB_USER,
        "regions": REGIONS,
        "project": PROJECT_ID,
        "minCores": 1,
        "image": f"gcr.io/{PROJECT_ID}/pgxpop/extract-pack:1.0",
        "logging": f"gs://{LOG_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/logs",
        "command": (
                    "python3 /pgxpop-pack/extract-pack/main.py " +
                    "--vcf ${VCF} " +
                    "--bed ${BED} " +
                    "--fasta ${FASTA_REF} " +
                    "--output ${BCF} " +
                    "--index ${BCF_INDEX}"),
        "envs": {
        #    "SAMPLE_ID": sample
        },
        "inputs": {
            "VCF": f"gs://{bucket}/{path}",
            "VCF_INDEX": f"gs://{tbi.bucket}/{tbi.path}",
            "BED": bed,
            "FASTA_REF": fasta_ref,
            "FASTA_INDEX": fasta_index
        },
        "outputs": {
            "BCF": f"gs://{OUT_BUCKET}/{task_name}/{regions_label}/output/{sample}.{regions_label}.bcf",
            "BCF_INDEX": f"gs://{OUT_BUCKET}/{task_name}/{regions_label}/output/{sample}.{regions_label}.bcf.csi"
        },
        "trellisTaskId": task_id,
        "sample": sample,
        "plate": plate,
        "name": task_name,
        "inputHash": trunc_nodes_hash,
        "labels": ["Job", "Dsub", unique_task_label],
        # Used to connect jobs to input nodes in the graph db
        "inputIds": [vcf['id'], tbi['id']],
        "network": NETWORK,
        "subnetwork": SUBNETWORK,       
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
        "--min-cores", str(job_dict["minCores"]), 
        "--logging", job_dict["logging"],
        "--image", job_dict["image"],
        "--use-private-address",
        "--network", job_dict["network"],
        "--subnetwork", job_dict["subnetwork"],        
        "--command", job_dict["command"],
    ]

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

        