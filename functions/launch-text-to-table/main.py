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

from datetime import datetime

from google.cloud import pubsub
from google.cloud import storage

from dsub.commands import dsub

#project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', '')
#zones = os.environ.get('ZONES', '')
#out_bucket = os.environ.get('DSUB_OUT_BUCKET', '')
#log_bucket = os.environ.get('DSUB_LOG_BUCKET', '')
#out_root = os.environ.get('DSUB_OUT_ROOT', '')
#DSUB_USER = os.environ.get('DSUB_USER', '')

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

    PUBLISHER = pubsub.PublisherClient()

class FastqcTask:

    def __init__(self, node):

        chromosome = node['chromosome']

        self.name = 'text-to-table'
        self.group = 'bam-fastqc' 
        self.schema_name = 'fastqc'
        self.json_schema = 't2t-fastqc.json'
        self.series = f'chromosome_{chromosome}'

class FlagstatTask:

    def __init__(self, node):

        chromosome = node['chromosome']

        self.name = 'text-to-table'
        self.group = 'flagstat'
        self.schema_name = 'flagstat'
        self.json_schema = 't2t-flagstat.json'
        self.series = f'chromosome_{chromosome}'

class VcfstatsTask:

    def __init__(self, node):

        self.name = 'text-to-table'
        self.group = 'vcfstats'
        self.schema_name = 'rtg_vcfstats'
        self.json_schema = 't2t-rtg-vcfstats.json'
        self.series = 'rtg_vcfstats.txt'

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


def launch_text_to_table(event, context):
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

    node = body['results'].get('node')
    if not node:
        print("> No node provided. Exiting.")
        return

    # Check validity of input node
    required_labels = ['Blob', 'Text']
    # Check whether blob label is supported
    supported_types = {
                       'Fastqc': FastqcTask, 
                       'Flagstat': FlagstatTask, 
                       'Vcfstats': VcfstatsTask
    }

    conditions = [
        # Check that all required labels are present
        set(required_labels).issubset(set(node.get('labels'))),
        # Check that one & only one type format class is represented
        len(set(supported_types.keys()).intersection(set(node.get('labels'))))==1,
    ]

    for condition in conditions:
        if condition:
            continue
        else:
            logging.error(f"Input node does not match requirements. Node: {node}.")
            return False

    task_label = supported_types.keys().intersection(set(node.get('labels'))).pop()

    #task_labels = set(supported_labels.keys()).intersection(set(labels))
    #if not task_labels:
    #    print(f"Info: Not a supported object. Ignoring {node}.")
    #    return 
    #elif len(task_labels) > 1:
    #    print(f"Error: Blob matches multiple labels. Ignoring {node}.")
    #    return
    #label = task_labels.pop()

    # Get task-specific metadata
    task = supported_types[task_label](node)
    task_name = task.name
    task_group = task.group
    schema_name = task.schema_name
    series = task.series
    json_schema = task.json_schema

    # Get task-generic metadata
    datetime_stamp = get_datetime_stamp()
    task_id, trunc_nodes_hash = make_unique_task_id([node], datetime_stamp)

    bucket = node['bucket']
    path = node['path']
    
    sample = node['sample']
    basename = node['basename']

    task_name = "text-to-table"
    unique_task_label = "TextToTable"
    job_dict = {
             "provider": "google-v2",
             "user": DSUB_USER,
             "regions": REGIONS,
             "project": PROJECT_ID,
             "minCores": 1,
             "image": f"gcr.io/{PROJECT_ID}/text-to-table:0.2.1",
             "logging": f"gs://{LOG_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/logs",
             "command": "text2table -j ${JSON} -o ${OUTPUT} -v series=${SERIES},sample=${SAMPLE_ID} ${INPUT}",
             "envs": {
                    "SAMPLE_ID": sample,
                    "SCHEMA": schema_name,
                    "SERIES": series,
             },
             "inputs": {
                    "INPUT": f"gs://{bucket}/{path}"
             },
             "outputs": {
                    "OUTPUT": f"gs://{OUT_BUCKET}/{plate}/{sample}/{task_name}/{task_id}/output/{task_group}/{basename}.csv"
             },
             "trellisTaskId": task_id,
             "sample": sample,
             "plate": plate,
             "name": task_name,
             "inputHash": trunc_nodes_hash,
             "labels": ["Job", "Dsub", unique_task_label],
             "inputIds": [node['id']],
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
        "--provider", job_dict["provider"],
        "--user", job_dict["user"],
        "--regions", job_dict["regions"], 
        "--project", job_dict["project"],
        "--min-cores", job_dict["minCores"], 
        "--logging", job_dict["logging"],
        "--image", job_dict["image"], 
        "--use-private-address",
        "--network", job_dict["network"],
        "--subnetwork", job_dict["subnetwork"],        
        "--command", job_dict["command"],
        #"--input", 
        #    f"JSON=gs://{out_bucket}/schemas/{json_schema}",
        #"--input", 
        #    f"INPUT=gs://{in_bucket}/{in_path}",
        #"--output", 
        #    f"OUTPUT=gs://{out_bucket}/{out_root}/{task_group}/{task_name}/objects/{basename}.csv",
        #"--env", f"SAMPLE_ID={sample}", 
        #"--env", f"SCHEMA={schema_name}", 
        #"--env", f"SERIES={series}"
    ]
    #print(dsub_args)
    #launch_dsub_task(dsub_args)

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

# For local testing
if __name__ == "__main__":
    project_id = "gbsc-gcp-project-mvp-dev"
    zones =  "us-west1*"
    out_bucket = "gbsc-gcp-project-mvp-dev-from-personalis-qc"
    out_root = "dsub"

    # Fastqc data
    data = {"resource": "blob", "gcp-metadata": {"bucket": "gbsc-gcp-project-mvp-dev-from-personalis-qc", "contentLanguage": "en", "contentType": "application/octet-stream", "crc32c": "XPwmhA==", "etag": "CMTx8Y/t8+ACEAE=", "generation": "1552093034608836", "id": "gbsc-gcp-project-mvp-dev-from-personalis/SHIP4420818/Alignments/SHIP4420818_chromosome_Y.recal.bam/1552093034608836", "kind": "storage#object", "md5Hash": "qm+p+AbPhpyXwmXtAQ/fkA==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/SHIP4420818%2FAlignments%2FSHIP4420818_chromosome_Y.recal.bam?generation=1552093034608836&alt=media", "metadata": {"goog-reserved-file-mtime": "1527072114"}, "metageneration": "1", "name": "dsub/fastqc-bam/fastqc/objects/SHIP4420818_chromosome_Y.recal.bam.fastqc_data.txt", "selfLink": "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/SHIP4420818%2FAlignments%2FSHIP4420818_chromosome_Y.recal.bam", "size": "921001444", "storageClass": "REGIONAL", "timeCreated": "2019-03-09T00:57:14.607Z", "timeStorageClassUpdated": "2019-03-09T00:57:14.607Z", "updated": "2019-03-09T00:57:14.607Z"}, "trellis-metadata": {"path": "SHIP4420818/Alignments/SHIP4420818_chromosome_Y.recal.bam", "dirname": "SHIP4420818/Alignments", "basename": "SHIP4420818_chromosome_Y.recal.bam.fastqc_data.txt", "extension": "recal.bam.fastqc_data.txt", "time-created-epoch": 1552093034.607, "time-updated-epoch": 1552093034.607, "time-created-iso": "2019-03-09T00:57:14.607000+00:00", "time-updated-iso": "2019-03-09T00:57:14.607000+00:00", "labels": ["Fastqc_data", "WGS_9000", "Blob"], "sample": "SHIP4420818", "chromosome": "Y"}}
    data = json.dumps(data)
    data = data.encode('utf-8')
    
    event = {'data': base64.b64encode(data)}

    launch_text_to_table(event, context=None)

    # Flagstat data
    data = {"resource": "blob", "gcp-metadata": {"bucket": "gbsc-gcp-project-mvp-dev-from-personalis-qc", "contentLanguage": "en", "contentType": "application/octet-stream", "crc32c": "XPwmhA==", "etag": "CMTx8Y/t8+ACEAE=", "generation": "1552093034608836", "id": "gbsc-gcp-project-mvp-dev-from-personalis/SHIP4420818/Alignments/SHIP4420818_chromosome_Y.recal.bam/1552093034608836", "kind": "storage#object", "md5Hash": "qm+p+AbPhpyXwmXtAQ/fkA==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/SHIP4420818%2FAlignments%2FSHIP4420818_chromosome_Y.recal.bam?generation=1552093034608836&alt=media", "metadata": {"goog-reserved-file-mtime": "1527072114"}, "metageneration": "1", "name": "dsub/flagstat/samtools/objects/SHIP4420818_chromosome_Y.recal.bam.flagstat.tsv", "selfLink": "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/SHIP4420818%2FAlignments%2FSHIP4420818_chromosome_Y.recal.bam", "size": "921001444", "storageClass": "REGIONAL", "timeCreated": "2019-03-09T00:57:14.607Z", "timeStorageClassUpdated": "2019-03-09T00:57:14.607Z", "updated": "2019-03-09T00:57:14.607Z"}, "trellis-metadata": {"path": "SHIP4420818/Alignments/SHIP4420818_chromosome_Y.recal.bam", "dirname": "SHIP4420818/Alignments", "basename": "SHIP4420818_chromosome_Y.recal.bam.fastqc_data.txt", "extension": "recal.bam.fastqc_data.txt", "time-created-epoch": 1552093034.607, "time-updated-epoch": 1552093034.607, "time-created-iso": "2019-03-09T00:57:14.607000+00:00", "time-updated-iso": "2019-03-09T00:57:14.607000+00:00", "labels": ["Flagstat_data", "WGS_9000", "Blob"], "sample": "SHIP4420818", "chromosome": "Y"}}
    data = json.dumps(data)
    data = data.encode('utf-8')
    
    event = {'data': base64.b64encode(data)}

    launch_text_to_table(event, context=None)

    # Vcfstats data
    data = {"resource": "blob", "gcp-metadata": {"bucket": "gbsc-gcp-project-mvp-dev-from-personalis-qc", "contentLanguage": "en", "contentType": "application/octet-stream", "crc32c": "XPwmhA==", "etag": "CMTx8Y/t8+ACEAE=", "generation": "1552093034608836", "id": "gbsc-gcp-project-mvp-dev-from-personalis/SHIP4420818/Alignments/SHIP4420818_chromosome_Y.recal.bam/1552093034608836", "kind": "storage#object", "md5Hash": "qm+p+AbPhpyXwmXtAQ/fkA==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/SHIP4420818%2FAlignments%2FSHIP4420818_chromosome_Y.recal.bam?generation=1552093034608836&alt=media", "metadata": {"goog-reserved-file-mtime": "1527072114"}, "metageneration": "1", "name": "dsub/vcfstats/rtg-tools/objects/SHIP3935743_rtg_vcfstats.txt", "selfLink": "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/SHIP4420818%2FAlignments%2FSHIP4420818_chromosome_Y.recal.bam", "size": "921001444", "storageClass": "REGIONAL", "timeCreated": "2019-03-09T00:57:14.607Z", "timeStorageClassUpdated": "2019-03-09T00:57:14.607Z", "updated": "2019-03-09T00:57:14.607Z"}, "trellis-metadata": {"path": "dsub/vcfstats/rtg-tools/objects/SHIP3935743_rtg_vcfstats.txt", "dirname": "dsub/vcfstats/rtg-tools/objects", "basename": "SHIP3935743_rtg_vcfstats.txt", "extension": ".txt", "time-created-epoch": 1552093034.607, "time-updated-epoch": 1552093034.607, "time-created-iso": "2019-03-09T00:57:14.607000+00:00", "time-updated-iso": "2019-03-09T00:57:14.607000+00:00", "labels": ["Vcfstats_data", "WGS_9000", "Blob"], "sample": "SHIP3935743"}}
    data = json.dumps(data)
    data = data.encode('utf-8')
    
    event = {'data': base64.b64encode(data)}

    launch_text_to_table(event, context=None)
