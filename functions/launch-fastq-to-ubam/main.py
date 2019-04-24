import os
import sys
import json
import yaml
import base64

from google.cloud import storage

from datetime import datetime

from dsub.commands import dsub

ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    ZONES = parsed_vars['DSUB_ZONES']
    OUT_BUCKET = parsed_vars['DSUB_OUT_BUCKET']
    LOB_BUCKET = parsed_vars['DSUB_LOG_BUCKET']
    OUT_ROOT = parsed_vars['DSUB_OUT_ROOT']
    DSUB_USER = parsed_vars['DSUB_USER']

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

def get_datestamp():
    now = datetime.now()
    datestamp = now.strftime("%Y%m%d")
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
    print(data)

    resource_type = 'nodes'
    if data['resource'] != resource_type:
        print(f"Error: Expected resource type '{resource_type}', " +
              f"got '{data['resource']}.'")
        return

    nodes = data['neo4j-metadata']['nodes']

    if len(nodes) != 2:
        print(f:"Error: Need 2 fastqs; {len(nodes)} provided.")
        return
    #if not 'Fast' in node['labels']:
    #    print(f"Info: Not a Bam object. Ignoring. {node}.")
    #    return

    # Dsub data
    task_name = 'fastq-to-ubam'
    task_group = 'gatk-5-dollar'

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

    datestamp = get_datestamp()

    dsub_args = [
        "--provider", "google-v2", 
        "--user", DSUB_USER, 
        "--zones", ZONES, 
        "--project", PROJECT_ID,
        "--min-cores", 1, 
        "--min-ram", 7.5,
        "--preemptible",
        "--boot-disk-size", 20, 
        "--image", 
            f"gcr.io/{PROJECT_ID}/jinasong/wdl_runner:latest", 
        "--logging", 
            f"gs://{LOG_BUCKET}/{OUT_ROOT}/{task_group}/{task_name}/{sample}/logs",
        "--disk-size", "1000",
        "--env", f"RG={read_group}",
        "--env", f"SM={sample}",
        "--env",  "PL=illumina",
        "--command", '/gatk/gatk --java-options "-Xmx8G -Djava.io.tmpdir=bla" FastqToSam -F1 ${FASTQ_1} -F2 ${FASTQ_2} -O ${UBAM} -RG ${RG} -SM ${SM} -PL ${PL}',
        "--output", 
            f"UBAM=gs://{OUT_BUCKET}/{OUT_ROOT}/{task_group}/{task_name}/{sample}/objects/{sample}_{index}.ubam",
    ]
    # Add fastqs as inputs
    for name, path in fastqs.items():
        dsub_args.extend(["--input", f"{name}={path}"])

    print(f"Launching dsub with args: {dsub_args}.")
    launch_dsub_task(dsub_args)

# For local testing
if __name__ == "__main__":
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"
    ZONES =  "us-west1*"
    OUT_BUCKET = "gbsc-gcp-project-mvp-dev-from-personalis-qc"
    OUT_ROOT = "dsub"


    launch_fastq_to_ubam(event, context=None)
