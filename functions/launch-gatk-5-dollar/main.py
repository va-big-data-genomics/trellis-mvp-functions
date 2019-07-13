import os
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
    #ZONES = parsed_vars['DSUB_ZONES']
    REGIONS = parsed_vars['DSUB_REGIONS']
    OUT_BUCKET = parsed_vars['DSUB_OUT_BUCKET']
    LOG_BUCKET = parsed_vars['DSUB_LOG_BUCKET']
    DSUB_USER = parsed_vars['DSUB_USER']
    TRELLIS_BUCKET = parsed_vars['TRELLIS_BUCKET']
    GATK_INPUTS_PATH = parsed_vars['GATK_HG38_INPUTS']
    # TODO: Create this
    NEW_JOBS_TOPIC = parsed_vars['NEW_JOBS_TOPIC']

    # Establish PubSub connection
    PUBLISHER = pubsub.PublisherClient()
    #TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{TOPIC}"


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    data = json.dumps(data).encode('utf-8')
    PUBLISHER.publish(topic_path, data=data)


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
    datestamp = now.strftime("%y%m%d-%H%M%S-%f")[:-3]
    return datestamp


def parse_case_results(results):
    """20190701: DEPRECATED with new cypher query
    """
    #'results': [{'CASE WHEN ': [{node_metadata}]}]
    results = results[0]
    return list(results.values())[0]


def format_create_node_query(db_entry, dry_run=False):
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


def launch_gatk_5_dollar(event, context):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(f"> Message: {pubsub_message}.")
    data = json.loads(pubsub_message)
    print(f"> Context: {context}.")
    print(f"> Data: {data}.")
    header = data['header']
    body = data['body']

    dry_run = header.get('dryRun')
    if not dry_run:
        dry_run = False

    #metadata = {}
    if len(body['results']) == 0:
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
    nodes_hash = hashlib.sha256(json.dumps(nodes).encode('utf-8')).hexdigest()
    task_id = f"{datetime_stamp}-{nodes_hash[:8]}"

    ubams = []
    for node in nodes:
        if 'Ubam' not in node['labels']:
            print(f"Error: inputs must be ubams.")
            return

        plate = node['plate']
        sample = node['sample']

        bucket = node['bucket']
        path = node['path']

        ubam_path = f"gs://{bucket}/{path}"
        ubams.append(ubam_path)

    # Load inputs JSON from GCS
    gatk_input_template = storage.Client(project=PROJECT_ID) \
        .get_bucket(TRELLIS_BUCKET) \
        .blob(GATK_INPUTS_PATH) \
        .download_as_string()
    gatk_inputs = json.loads(gatk_input_template)

    # Add key/values
    gatk_inputs['germline_single_sample_workflow.sample_name'] = sample
    gatk_inputs['germline_single_sample_workflow.base_file_name'] = sample
    gatk_inputs['germline_single_sample_workflow.flowcell_unmapped_bams'] = ubams
    gatk_inputs['germline_single_sample_workflow.final_vcf_base_name'] = sample

    # Write JSON to GCS
    gatk_inputs_path = f"{plate}/{sample}/{task_name}/{task_id}/inputs/inputs.json"
    gatk_inputs_blob = storage.Client(project=PROJECT_ID) \
        .get_bucket(OUT_BUCKET) \
        .blob(gatk_inputs_path) \
        .upload_from_string(json.dumps(gatk_inputs, indent=4))
    print(f"Created input blob at gs://{OUT_BUCKET}/{gatk_inputs_path}.")

    workflow_inputs_path = "workflow-inputs/gatk-mvp/gatk-mvp-pipeline"
    job_dict = {
                "provider": "google-v2",
                "user": DSUB_USER,
                #"zones": ZONES,
                "regions": REGIONS,
                "project": PROJECT_ID,
                "minCores": 1,
                "minRam": 6.5,
                "preemptible": False,
                "bootDiskSize": 20,
                "image": f"gcr.io/{PROJECT_ID}/***REMOVED***/wdl_runner:latest",
                "logging": f"gs://{LOG_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/logs",
                "diskSize": 1000,
                "command": ("java " +
                            "-Dconfig.file=${CFG} " +
                            "-Dbackend.providers.JES.config.project=${MYproject} " +
                            "-Dbackend.providers.JES.config.root=${ROOT} " +
                            "-jar /cromwell/cromwell.jar " +
                            "run ${WDL} " +
                            "--inputs ${INPUT} " +
                            "--options ${OPTION}"
                ),
                "inputs": {
                           "CFG": f"gs://{TRELLIS_BUCKET}/{workflow_inputs_path}/google-adc.conf", 
                           "OPTION": f"gs://{TRELLIS_BUCKET}/{workflow_inputs_path}/generic.google-papi.options.json",
                           "WDL": f"gs://{TRELLIS_BUCKET}/{workflow_inputs_path}/fc_germline_single_sample_workflow.wdl",
                           "SUBWDL": f"gs://{TRELLIS_BUCKET}/{workflow_inputs_path}/tasks_pipelines/*.wdl",
                           "INPUT": f"gs://{OUT_BUCKET}/{gatk_inputs_path}",
                },
                "envs": {
                         "MYproject": PROJECT_ID,
                         "ROOT": f"gs://{OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output",
                },
                "preemptible": False,
                "dryRun": dry_run,
                "taskId": task_id,
                "sample": sample,
                "plate": plate,
                "name": task_name,
    }

    dsub_args = [
                 "--name", job_dict["name"],
                 "--label", f"sample={sample.lower()}",
                 "--label", f"trellis-id={task_id}",
                 "--label", f"plate={plate.lower()}",
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

    print(f"Launching dsub with args: {dsub_args}.")
    result = launch_dsub_task(dsub_args)
    print(f"Dsub result: '{result}'.")

    # If job launch is successful, add job to database
    if result == 1:
        # Add additional job metadata
        job_dict['labels'] = ['Job', 'Cromwell']

        # Reformat dict values as separate key/value pairs
        # to be compatible with Neo4j
        for key, value in job_dict["inputs"].items():
            job_dict[f"input_{key}"] = value
        for key, value in job_dict["envs"].items():
            job_dict[f"env_{key}"] = value

        # Package job node and inputs into JSON message
        message = {
            "header": {
                "method": "POST",
                "labels": ["Job", "Cromwell", "Command", "Args", "Inputs"],
                "resource": "job-metadata",
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
        publish_to_topic(NEW_JOBS_TOPIC, message)  

        # Write message to blob
        storage.Client(project=PROJECT_ID) \
            .get_bucket(TRELLIS_BUCKET) \
            .blob('launch-gatk-5-dollar-message.out') \
            .upload_from_string(json.dumps(message))
        #gatk_inputs = json.loads(gatk_input_template)


# For local testing
if __name__ == "__main__":
    PROJECT_ID = "***REMOVED***-dev"
    ZONES =  "us-west1*"
    TRELLIS_BUCKET = "***REMOVED***-dev-trellis"
    GATK_INPUTS_PATH = "workflow-inputs/gatk-mvp/mvp.hg38.inputs.json"
    OUT_BUCKET = "***REMOVED***-dev-from-personalis-gatk"
    LOG_BUCKET = "***REMOVED***-dev-from-personalis-gatk-logs"
    DSUB_USER = "trellis"
    NEW_JOBS_TOPIC = "wgs35-new-jobs"

    PUBLISHER = pubsub.PublisherClient()

    data = {
        'header': {
            'method': 'VIEW',
            'labels': ['Ubam', 'Nodes'],
            'resource': 'query-result',
            'sentFrom': 'db-query',
            'dryRun': True,
        },
        'body': {
            'query': 'MATCH (n:Ubam) WHERE n.sample="SHIP4946367" WITH n.sample AS sample, COLLECT(n) as nodes RETURN CASE WHEN size(nodes) = 4 THEN nodes ELSE NULL END',
            'results': [
                {
                    'CASE \nWHEN size(nodes) = 4 \nTHEN nodes \nELSE NULL \nEND': [
                        {
                            'basename': 'SHIP4946367_2.ubam',
                            'bucket': '***REMOVED***-dev-from-personalis-gatk',
                            'contentType': 'application/octet-stream',
                            'crc32c': 'ojStVg==',
                            'dirname': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output',
                            'etag': 'CJTpxe3ynuICEAM=',
                            'extension': 'ubam',
                            'generation': '1557970088457364',
                            'id': '***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam/1557970088457364',
                            'kind': 'storage#object',
                            'labels': ['WGS35', 'Blob', 'Ubam'],
                            'md5Hash': 'opGAi0f9olAu4DKzvYiayg==',
                            'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam?generation=1557970088457364&alt=media',
                            'metageneration': '3',
                            'name': 'SHIP4946367_2',
                            'path': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam',
                            'sample': 'SHIP4946367',
                            'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam',
                            'size': 16886179620,
                            'storageClass': 'REGIONAL',
                            'timeCreated': '2019-05-16T01:28:08.455Z',
                            'timeCreatedEpoch': 1557970088.455,
                            'timeCreatedIso': '2019-05-16T01:28:08.455000+00:00',
                            'timeStorageClassUpdated': '2019-05-16T01:28:08.455Z',
                            'timeUpdatedEpoch': 1558045261.522,
                            'timeUpdatedIso': '2019-05-16T22:21:01.522000+00:00',
                            'trellisTask': 'fastq-to-ubam',
                            'trellisWorkflow': 'fastq-to-vcf',
                            'updated': '2019-05-16T22:21:01.522Z'
                        },
                        {
                            'basename': 'SHIP4946367_0.ubam',
                            'bucket': '***REMOVED***-dev-from-personalis-gatk',
                            'contentType': 'application/octet-stream',
                            'crc32c': 'ZaJM+g==',
                            'dirname': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output',
                            'etag': 'CM+sxKDynuICEAY=',
                            'extension': 'ubam',
                            'generation': '1557969926952527',
                            'id': '***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam/1557969926952527',
                            'kind': 'storage#object',
                            'labels': ['WGS35', 'Blob', 'Ubam'],
                            'md5Hash': 'Tgh+eyIiKe8TRWV6vohGJQ==',
                            'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam?generation=1557969926952527&alt=media',
                            'metageneration': '6',
                            'name': 'SHIP4946367_0',
                            'path': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam',
                            'sample': 'SHIP4946367',
                            'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam',
                            'size': 16871102587,
                            'storageClass': 'REGIONAL',
                            'timeCreated': '2019-05-16T01:25:26.952Z',
                            'timeCreatedEpoch': 1557969926.952,
                            'timeCreatedIso': '2019-05-16T01:25:26.952000+00:00',
                            'timeStorageClassUpdated': '2019-05-16T01:25:26.952Z',
                            'timeUpdatedEpoch': 1558045265.901,
                            'timeUpdatedIso': '2019-05-16T22:21:05.901000+00:00',
                            'trellisTask': 'fastq-to-ubam',
                            'trellisWorkflow': 'fastq-to-vcf',
                            'updated': '2019-05-16T22:21:05.901Z'
                        }
                    ]
                }
            ]
        }
    }

    with open('test-inputs.json', 'w') as fh:
        fh.write(json.dumps(data))

    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}

    result = launch_gatk_5_dollar(event, context=None)