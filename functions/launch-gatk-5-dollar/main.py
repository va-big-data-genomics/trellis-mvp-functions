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
    GATK_MVP_DIR = parsed_vars['GATK_MVP_DIR']
    GATK_MVP_HASH = parsed_vars['GATK_MVP_HASH']
    GATK_GERMLINE_DIR = parsed_vars['GATK_GERMLINE_DIR']
    CROMWELL_IMAGE = parsed_vars['CROMWELL_IMAGE']

    NEW_JOBS_TOPIC = parsed_vars['NEW_JOBS_TOPIC']

    # Establish PubSub connection
    PUBLISHER = pubsub.PublisherClient()


def format_pubsub_message(job_dict, seed_id, event_id):
    message = {
        "header": {
            "resource": "job-metadata",
            "method": "POST",
            "labels": ["Create", "Job", "CromwellWorkflow", "Dsub", "Node"],
            "sentFrom": f"{FUNCTION_NAME}",
            "seedId": f"{seed_id}",
            "previousEventId": f"{event_id}"
        },
        "body": {
            "node": job_dict,
        }
    }
    return message


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    data = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=data)
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
    # Create pretty-unique hash value based on input nodes
    # https://www.geeksforgeeks.org/ways-sort-list-dictionaries-values-python-using-lambda-function/
    sorted_nodes = sorted(nodes, key = lambda i: i['id'])
    nodes_str = json.dumps(sorted_nodes, sort_keys=True, ensure_ascii=True, default=str)
    nodes_hash = hashlib.sha256(nodes_str.encode('utf-8')).hexdigest()
    print(nodes_hash)
    trunc_nodes_hash = str(nodes_hash)[:8]
    task_id = f"{datetime_stamp}-{trunc_nodes_hash}"
    return(task_id, trunc_nodes_hash)


def launch_gatk_5_dollar(event, context):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    logging.info(f"> Received PubSub Message: {pubsub_message}.")
    data = json.loads(pubsub_message)
    logging.info(f"> Context: {context}.")
    logging.info(f"> Data: {data}.")
    header = data['header']
    body = data['body']

    seed_id = header['seedId']
    event_id = context.event_id

    dry_run = header.get('dryRun')
    if not dry_run:
        dry_run = False

    #metadata = {}
    if len(body['results']) == 0:
        # This is expected, because everytime a ubam is created 
        # a query result will be sent here, but will be empty 
        # unless all ubams are present
        logging.warn("No results found; ignoring.")
        return
    elif len(body['results']) != 1:
        raise ValueError(f"Expected single result, got {len(body['results'])}.")
    else:
        pass
    
    nodes = body['results']['nodes']
    # If not all Ubams present in database, results will be NoneType
    if not nodes:
        raise ValueError("No nodes provided; exiting.")

    # Dsub data
    task_name = 'gatk-5-dollar'
    # Create unique task ID
    datetime_stamp = get_datetime_stamp()
    task_id, trunc_nodes_hash = make_unique_task_id(nodes, datetime_stamp)

    ubams = []
    # inputIds used to create relationships via trigger
    input_ids = []
    for node in nodes:
        if 'Ubam' not in node['labels']:
            print(f"Error: inputs must be ubams.")
            return

        plate = node['plate']
        sample = node['sample']
        input_id = node['id']

        bucket = node['bucket']
        path = node['path']

        ubam_path = f"gs://{bucket}/{path}"
        ubams.append(ubam_path)
        input_ids.append(input_id)

    # Load Pipeline API (PAPI) options JSON from GCS
    try:
        logging.info(f"> Loading PAPI options")
        gatk_papi_inputs = f"{GATK_MVP_DIR}/{GATK_MVP_HASH}/{GATK_GERMLINE_DIR}/generic.google-papi.options.json"
        papi_options_template = storage.Client(project=PROJECT_ID) \
            .get_bucket(TRELLIS_BUCKET) \
            .blob(gatk_papi_inputs) \
            .download_as_string()
        papi_options = json.loads(papi_options_template)
    except:
        logging.error(f"Failed to load PAPI options from {gatk_papi_inputs}.")

    # Add Trellis ID to Cromwell workers
    # NOTE: Doesn't work. Don't know where these are supposed to go.
    #papi_options["google_labels"] = {
    #                                 "trellis-id": task_id,
    #                                 "sample": sample
    #}

    # Write workflow-specific PAPI options to GCS
    try:
        logging.info(f"> Writing workflow-specific PAPI options to GCS")
        papi_options_path = f"{plate}/{sample}/{task_name}/{task_id}/inputs/{sample}.google-papi.options.json"
        papi_options_blob = storage.Client(project=PROJECT_ID) \
            .get_bucket(OUT_BUCKET) \
            .blob(papi_options_path) \
            .upload_from_string(json.dumps(papi_options, indent=4))
        logging.info(f"> Created PAPI options blob at gs://{OUT_BUCKET}/{papi_options_path}.")
    except:
        logging.error(f"Failed to write workflow-specific PAPI options to {papi_options_path}.")

    try:
        # Load inputs JSON from GCS
        logging.info(f"> Loading workflow inputs JSON from GCS")
        gatk_hg38_inputs = f"{GATK_MVP_DIR}/{GATK_MVP_HASH}/mvp.hg38.inputs.json"
        gatk_input_template = storage.Client(project=PROJECT_ID) \
            .get_bucket(TRELLIS_BUCKET) \
            .blob(gatk_hg38_inputs) \
            .download_as_string()
        gatk_inputs = json.loads(gatk_input_template)
    except:
        logging.error(f"Failed to load workflow inputs from {gatk_hg38_inputs}.")

    # Add key/values
    logging.info(f"> Adding sample-specific inputs to JSON")
    gatk_inputs['germline_single_sample_workflow.sample_name'] = sample
    gatk_inputs['germline_single_sample_workflow.base_file_name'] = sample
    gatk_inputs['germline_single_sample_workflow.flowcell_unmapped_bams'] = ubams
    gatk_inputs['germline_single_sample_workflow.final_vcf_base_name'] = sample

    # Write inputs JSON to GCS
    logging.info(f"> Write workflow inputs JSON back to GCS")
    gatk_inputs_path = f"{plate}/{sample}/{task_name}/{task_id}/inputs/inputs.json"
    gatk_inputs_blob = storage.Client(project=PROJECT_ID) \
        .get_bucket(OUT_BUCKET) \
        .blob(gatk_inputs_path) \
        .upload_from_string(json.dumps(gatk_inputs, indent=4))
    print(f"> Created input blob at gs://{OUT_BUCKET}/{gatk_inputs_path}.")

    #workflow_inputs_path = "workflow-inputs/gatk-mvp/gatk-mvp-pipeline"
    unique_task_label = "Gatk5Dollar"
    job_dict = {
                "provider": "google-cls-v2",
                "user": DSUB_USER,
                "regions": REGIONS,
                "project": PROJECT_ID,
                "minCores": 1,
                "minRam": 12,
                "preemptible": False,
                "bootDiskSize": 20,
                "image": f"gcr.io/{PROJECT_ID}/{CROMWELL_IMAGE}",
                "logging": f"gs://{LOG_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/logs",
                "diskSize": 100,
                "command": ("java " +
                            "-Dconfig.file=${CFG} " +
                            "-Dbackend.providers.${BACKEND_PROVIDER}.config.project=${PROJECT} " +
                            "-Dbackend.providers.${BACKEND_PROVIDER}.config.root=${ROOT} " +
                            "-jar /app/cromwell.jar " +
                            "run ${WDL} " +
                            "--inputs ${INPUT} " +
                            "--options ${OPTION}"
                ),
                "inputs": {
                           "CFG": f"gs://{TRELLIS_BUCKET}/{GATK_MVP_DIR}/{GATK_MVP_HASH}/{GATK_GERMLINE_DIR}/google-adc.conf", 
                           "OPTION": f"gs://{OUT_BUCKET}/{papi_options_path}",
                           "WDL": f"gs://{TRELLIS_BUCKET}/{GATK_MVP_DIR}/{GATK_MVP_HASH}/{GATK_GERMLINE_DIR}/fc_germline_single_sample_workflow.wdl",
                           "SUBWDL": f"gs://{TRELLIS_BUCKET}/{GATK_MVP_DIR}/{GATK_MVP_HASH}/{GATK_GERMLINE_DIR}/tasks_pipelines/*.wdl",
                           "INPUT": f"gs://{OUT_BUCKET}/{gatk_inputs_path}",
                },
                "envs": {
                         "PROJECT": PROJECT_ID,
                         "ROOT": f"gs://{OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output",
                         "BACKEND_PROVIDER": "PAPIv2"
                },
                "preemptible": False,
                "dryRun": dry_run,
                "trellisTaskId": task_id,
                "sample": sample,
                "plate": plate,
                "name": task_name,
                "inputHash": trunc_nodes_hash,
                "labels": [
                            'Job',
                            'Dsub',
                            'CromwellWorkflow',
                            unique_task_label],
                "inputIds": input_ids,
                "gatkMvpCommit": GATK_MVP_HASH,
                "network": NETWORK,
                "subnetwork": SUBNETWORK,
                "timeout": "48h"
    }

    dsub_args = [
                 "--name", f"gatk-{job_dict['inputHash'][0:5]}",
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
                 "--network", job_dict["network"],
                 "--subnetwork", job_dict["subnetwork"],
                 "--use-private-address",
                 "--enable-stackdriver-monitoring",
                 "--timeout", job_dict["timeout"]
    ]

    # Argument lists
    for key, value in job_dict['inputs'].items():
        dsub_args.extend([
                          "--input",
                          f"{key}={value}"]
        )
    for key, value in job_dict['envs'].items():
        dsub_args.extend([
                          "--env",
                          f"{key}={value}"]
        )
    
    # Optional flags
    if job_dict['preemptible']:
        dsub_args.append("--preemptible")
    if job_dict['dryRun']:
        dsub_args.append("--dry-run")

    print(f"> Launching dsub with args: {dsub_args}.")
    dsub_result = launch_dsub_task(dsub_args)
    print(f"> Dsub result: {dsub_result}.")

    # If job launch is successful, add job to database
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
        
        # Reformat dict values as separate key/value pairs
        # to be compatible with Neo4j
        for key, value in job_dict["inputs"].items():
            job_dict[f"input_{key}"] = value
        for key, value in job_dict["envs"].items():
            job_dict[f"env_{key}"] = value

        # Package job node and inputs into JSON message
        message = format_pubsub_message(
                                        job_dict = job_dict,
                                        seed_id = seed_id,
                                        event_id = event_id)
        print(f"> Pubsub message: {message}.")
        result = publish_to_topic(NEW_JOBS_TOPIC, message)  
        print(f"> Published message to {NEW_JOBS_TOPIC} with result: {result}.")
