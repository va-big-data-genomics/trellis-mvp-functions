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

import trellisdata as trellis

from google.cloud import storage
from google.cloud import pubsub

from datetime import datetime
from dsub.commands import dsub

ENVIRONMENT = os.environ.get('ENVIRONMENT')
if ENVIRONMENT == 'google-cloud':
    # Set up the Google Cloud Logging python client library
    # source: https://cloud.google.com/blog/products/devops-sre/google-cloud-logging-python-client-library-v3-0-0-release
    import google.cloud.logging
    client = google.cloud.logging.Client()
    # log_level=10 is equivalent to DEBUG; default is 20 == INFO
    # Gcloud Python logging client: https://googleapis.dev/python/logging/latest/client.html?highlight=setup_logging#google.cloud.logging_v2.client.Client.setup_logging
    # Logging levels: https://docs.python.org/3/library/logging.html#logging-levels
    client.setup_logging(log_level=10)

    # use Python's standard logging library to send logs to GCP
    import logging

    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    GCP_PROJECT = os.environ['GCP_PROJECT']

    # Load Trellis configuration
    config_doc = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    TRELLIS_CONFIG = yaml.safe_load(config_doc)

    # Load launcher configuration
    launcher_document = storage.Client() \
                        .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                        .get_blob(TRELLIS["JOB_LAUNCHER_CONFIG"]) \
                        .download_as_string()
    TASKS = yaml.load_all(launcher_document, Loader=yaml.FullLoader)

    PUBLISHER = pubsub.PublisherClient()

def _get_job_values(task, start, end, params):
    supported_value_types = {
        "int": int,
        "float": float
    }
    supported_params = [
        'inputs', 
        'env_variables'
    ]
    if not params in supported_params:
        raise ValueError(f"{params} is not in supported fields: {supported_params}")
    
    task_fields = task.dsub[params]
    
    sources = {
        "start": start['properties'],
        "end": end['properties']
    }

    # Inputs must provide either a "value" field with
    # a static value or a "template" and "source" fields
    # that will be used to generate value at runtime
    # from source [start,end] values.
    # Inspiration: https://stackoverflow.com/questions/54351740/how-can-i-use-f-string-with-a-variable-not-with-a-string-literal
    job_values = {}
    for key in task_fields:
        value = task_fields[key].get('value')
        if not value:
            source = task_fields[key]['source']
            template = task_fields[key]['template']
            value_type = task_fields[key].get('value_type')
            value = template.format(**sources[source])
            
            if value_type:
                if not value_type in supported_value_types.keys():
                    raise ValueError(f"Type {value_type} not in supported types: {supported_value_types.keys()}")
                else:
                    value = supported_value_types[value_type](value)
        job_values[key] = value
    return job_values

def _get_output_values(task, bucket, start, end, job_id):
    sources = {
        "start": start['properties'],
        "end": end['properties']
    }
    task_outputs = task.dsub['outputs']

    output_values = {}
    for key in task_outputs:
        value = task_outputs[key].get('value')
        if not value:
            source = task_outputs[key]['source']
            template = task_outputs[key]['template']
            value = template.format(**sources[source])
        value = f"gs://{bucket}/{task.name}/{job_id}/output/{value}"
        output_values[key] = value
    return output_values

def _get_label_values(task, start, end):
    sources = {
        "start": start['properties'],
        "end": end['properties']
    }
    task_labels = task.dsub['labels']

    label_values = {}
    for key in task_labels:
        value = task_labels[key].get('value')
        if not value:
            source = task_labels[key]['source']
            template = task_labels[key]['template']
            value = template.format(**sources[source])
        # Lowercase values required for GCP VM labels
        label_values[key.lower()] = value.lower()
    return label_values  

def parse_inputs(query_response):
    if not query_response.relationship:
        raise ValueError("Query response does not have relationship. " +
                         "Expected (Fastq)-[]->(Fastq).")
    r1 = query_response.relationship['start_node']
    rel = query_response.relationship['type']
    r2 = query_response.relationship['end_node']

    if not 'Fastq' in r1['labels'] and 'Fastq' in r2['labels']:
        raise ValueError(
                         "Both nodes do not have 'Fastq' labels." +
                         f"Fastq R1 labels: {r1['labels']}." +
                         f"Fastq R2 labels: {r2['labels']}.")

    if not r1['properties']['readGroup'] == r2['properties']['readGroup']:
        return ValueError(
                          "Fastqs are not from the same read group. " +
                          f"Fastq R1 read group: {r1['properties']['readGroup']}. " +
                          f"Fastq R2 read group: {r2['properties']['readGroup']}.")
    if not r1['properties']['matePair'] == 1 and r2['properties']['matePair'] == 2:
        return ValueError(
                          "Fastqs are not a correctly oriented mate pair. " +
                          f"Fastq R1 mate pair value (expect 1): {r1['properties']['matePair']}. " +
                          f"Fastq R2 mate pair value (expect 2): {r2['properties']['matePair']}.")
    
    fastq_fields = []
    for fastq in [r1, r2]:
        fastq_fields.extend([
                             fastq['properties']['plate'], 
                             fastq['properties']['sample'],
                             fastq['properties']['readGroup']])
    if len(set(fastq_fields)) != 3:
        raise ValueError(f"> Fastq fields are not in agreement: {fastq_fields}.")

    return r1, r2

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

def write_metadata_to_blob(meta_blob_path, metadata):
    try:
        meta_blob = storage.Client(project=PROJECT_ID) \
            .get_bucket(OUT_BUCKET) \
            .blob(meta_blob_path) \
            .upload_from_string(json.dumps(metadata))
        return True
    except:
        return False

def create_job_dict(task, start_node, end_node, job_id, input_ids, trunc_nodes_hash):

    env_variables = _get_job_values(task, start_node, end_node, "env_variables")
    inputs = _get_job_values(task, start_node, end_node, "inputs")
    outputs = _get_output_values(task, start_node, end_node)
    dsub_labels = _get_label_values(task, start_node, end_node)

    # Use camelcase keys for this dict because it will be added to Neo4j
    # database where camelcase is the standard.
    job_dict = {
        "name": task.name,
        "dsubName": f"{task.dsub_prefix}-{trunc_nodes_hash[0:5]}",
        "inputHash": trunc_nodes_hash,
        "inputIds": input_ids,
        "trellisTaskId": job_id,
        # Standard dsub configuration
        "provider": "google-v2",
        "user": TRELLIS_CONFIG['DSUB_USER'],
        "regions": TRELLIS_CONFIG['REGIONS'],
        "project": TRELLIS_CONFIG['PROJECT_ID'],
        "network": TRELLIS_CONFIG['NETWORK'],
        "subnetwork": TRELLIS_CONFIG['SUBNETWORK'],
        # Task specific dsub configuration
        "minCores": task.virtual_machine["min_cores"],
        "minRam": task.virtual_machine["min_ram"],
        "bootDiskSize": task.virtual_machine["boot_disk_size"],
        "image": f"gcr.io/{TRELLIS_CONFIG['PROJECT_ID']}/{task.virtual_machine['image']}",
        "logging": f"gs://{LOG_BUCKET}/{task.name}/{job_id}/logs",
        "diskSize": task.virtual_machine['disk_size'],
        "preemptible": task.dsub['preemptible'],
        "command": task.dsub['command'],
        # Parameterized values
        "envs": env_variables,
        "inputs": inputs,
        "outputs": outputs,
        "dsubLabels": dsub_labels
    }
    return job_dict

def launch_job(event, context):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """
    
    query_response = trellis.QueryResponseReader(
                        context = context,
                        event = event)

    print(f"> Received message (context): {query_response.context}.")
    print(f"> Message header: {query_response.header}.")
    print(f"> Message body: {query_response.body}.")

    if query_response.nodes:
        nodes = parse_node_inputs(query_response)
    elif query_response.relationship:
        start, rel, end = parse_relationship_inputs(query_response)
        nodes = [start, end]

    job_id, trunc_nodes_hash = trellis.make_unique_task_id(nodes=nodes)
    
    # inputIds used to create relationships via trigger
    input_id = []
    for node in nodes:
        input_ids.append(node['id'])

    # Define logging & outputs after task_id
    task_name = query_response.job_request
    task = TASKS[task_name]
    """
    job_dict = {
                "provider": "google-v2",
                "user": TRELLIS_CONFIG['DSUB_USER'],
                "regions": TRELLIS_CONFIG['REGIONS'],
                "project": TRELLIS_CONFIG['PROJECT_ID'],
                "minCores": task.virtual_machine["min_cores"],
                "minRam": task.virtual_machine["min_ram"],
                "bootDiskSize": task.virtual_machine["boot_disk_size"],
                "image": f"gcr.io/{TRELLIS_CONFIG['PROJECT_ID']}/{task.virtual_machine['image']}",
                "logging": f"gs://{LOG_BUCKET}/{task_name}/{task_id}/logs",
                "diskSize": task.virtual_machine['disk_size'],
                "command": task.dsub['command']
                "envs": task.environment_variables
                "inputs": {
                           "FASTQ_R1": f"gs://{fastq_r1['properties']['bucket']}/{fastq_r1['properties']['path']}",
                           "FASTQ_R2": f"gs://{fastq_r2['properties']['bucket']}/{fastq_r2['properties']['path']}"
                },
                "outputs": {
                            "UBAM": f"gs://{OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output/{sample}_{read_group}.ubam"
                },
                "trellisTaskId": task_id,
                "preemptible": task.dsub['preemptible'],
                #"sample": sample,
                #"plate": plate,
                #"readGroup": read_group,
                "name": task_name,
                "inputHash": trunc_nodes_hash,
                #"labels": ["Job", "Dsub", unique_task_label],
                "inputIds": input_ids,
                "network": TRELLIS_CONFIG['NETWORK'],
                "subnetwork": TRELLIS_CONFIG['SUBNETWORK'],
    }
    """
    job_dict = create_job_dict(task, job_id, input_ids, trunc_nodes_hash)

    dsub_args = [
        "--name", job_dict["dsubName"],
        #"--label", f"read-group={read_group}",
        #"--label", f"sample={sample.lower()}",
        #"--label", f"plate={plate.lower()}",
        "--label", f"trellis-id={task_id}",
        "--label", f"trellis-name={job_dict['name']}",
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
    for key, value in job_dict['dsubLabels'].items():
        dsub_args.extend([
                          "--label",
                          f"{key}={value}"])

    # Optional flags
    if not TRELLIS_CONFIG['ENABLE_JOB_LAUNCH']:
        dsub_args.append("--dry-run")

    # TODO: Need to handle this issue with database logic
    # Wait a random time interval to reduce overlapping db queries
    #   because of ubam objects created at same time.
    #random_wait = random.randrange(0,10)
    #print(f"> Waiting for {random_wait} seconds to launch job.")
    #time.sleep(random_wait)
    
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
        """
        message = format_pubsub_message(
                                        job_dict = job_dict, 
                                        #nodes = nodes,
                                        seed_id = seed_id,
                                        event_id = event_id)
        """

        # 1.3 update
        message = trellis.JobCreatedWriter(
            sender = FUNCTION_NAME,
            seed_id = query_response.seed_id,
            previous_event_id = query_response.event_id,
            job_dict = job_dict)

        print(f"> Pubsub message: {message}.")
        result = trellis.utils.publish_to_pubsub_topic(
                    publisher = PUBLISHER,
                    project_id = PROJECT_ID,
                    topic = NEW_JOB_TOPIC,
                    message = message) 
        print(f"> Published message to {NEW_JOB_TOPIC} with result: {result}.")
