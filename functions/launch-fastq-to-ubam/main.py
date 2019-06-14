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
    print(f"> Context: {context}.")
    print(f"> Data: {data}.")
    header = data['header']
    body = data['body']

    metadata = {}
    nodes = body['results']['nodes']
    for result_name in body['results']:
        elements = result_name.split('_')
        if elements[0] == 'metadata':
            key = elements[1]
            metadata[key] = data['results'][result_name]

    if len(nodes) != 2:
        print(f"Error: Need 2 fastqs; {len(nodes)} provided.")
        return

    # Dsub data
    task_name = 'fastq-to-ubam'
    workflow_name = 'fastq-to-vcf'

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
        "--min-cores", "1", 
        "--min-ram", "7.5",
        "--preemptible",
        "--boot-disk-size", "20", 
        "--image", 
            f"gcr.io/{PROJECT_ID}/broadinstitute/gatk:4.1.0.0", 
        "--logging", 
            f"gs://{LOG_BUCKET}/{sample}/{workflow_name}/{task_name}/logs",
        "--disk-size", "1000",
        "--env", f"RG={read_group}",
        "--env", f"SM={sample}",
        "--env",  "PL=illumina",
        "--command", '/gatk/gatk --java-options "-Xmx8G -Djava.io.tmpdir=bla" FastqToSam -F1 ${FASTQ_1} -F2 ${FASTQ_2} -O ${UBAM} -RG ${RG} -SM ${SM} -PL ${PL}',
        "--output", 
            f"UBAM=gs://{OUT_BUCKET}/{sample}/{workflow_name}/{task_name}/objects/{sample}_{read_group}.ubam",
    ]

    # Add fastqs as inputs
    for name, path in fastqs.items():
        dsub_args.extend(["--input", f"{name}={path}"])

    print(f"Launching dsub with args: {dsub_args}.")
    result = launch_dsub_task(dsub_args)
    print(f"Dsub result: '{result}'.")

    if result == 1 and metadata:
        print(f"Metadata passed to output blobs: {metadata}.")
        # Dump metadata into GCS blob
        meta_blob_path = f"{sample}/{workflow_name}/{task_name}/metadata/all-objects.json"
        meta_blob = storage.Client(project=PROJECT_ID) \
            .get_bucket(OUT_BUCKET) \
            .blob(meta_blob_path) \
            .upload_from_string(json.dumps(metadata))
        print(f"Created metadata blob at gs://{OUT_BUCKET}/{meta_blob_path}.")

# For local testing
if __name__ == "__main__":
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"
    ZONES =  "us-west1*"
    OUT_BUCKET = "gbsc-gcp-project-mvp-dev-from-personalis-gatk"
    LOG_BUCKET = "gbsc-gcp-project-mvp-dev-from-personalis-gatk-logs"
    DSUB_USER = "trellis"

    data = {
            'resource': 'query-result', 
            'query': 'MATCH (n:Fastq) WHERE n.sample="SHIP4946367" WITH n.readGroup AS read_group, collect(n) AS nodes WHERE size(nodes) = 2 RETURN [n in nodes] AS nodes', 
            'results': {
                        'nodes': [
                                  {'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz', 'storageClass': 'REGIONAL', 'setSize': 9, 'timeCreatedEpoch': 1555361455.813, 'timeUpdatedEpoch': 1556919952.482, 'timeCreated': '2019-04-15T20:50:55.813Z', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565', 'contentType': 'application/octet-stream', 'generation': '1555361455813565', 'metageneration': '46', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T21:45:52.482000+00:00', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media', 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R1.fastq.gz', 'crc32c': 'ftNG8w==', 'size': 5955984357, 'timeStorageClassUpdated': '2019-04-15T20:50:55.813Z', 'name': 'SHIP4946367_0_R1', 'etag': 'CL3nyPj80uECEC4=', 'timeCreatedIso': '2019-04-15T20:50:55.813000+00:00', 'matePair': 1, 'updated': '2019-05-03T21:45:52.482Z'}, 
                                  {'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz', 'storageClass': 'REGIONAL', 'setSize': 9, 'timeCreatedEpoch': 1555361456.112, 'timeUpdatedEpoch': 1556920219.608, 'timeCreated': '2019-04-15T20:50:56.112Z', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz/1555361456112810', 'contentType': 'application/octet-stream', 'generation': '1555361456112810', 'metageneration': '16', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T21:50:19.608000+00:00', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz?generation=1555361456112810&alt=media', 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R2.fastq.gz', 'crc32c': 'aV17ew==', 'size': 6141826914, 'timeStorageClassUpdated': '2019-04-15T20:50:56.112Z', 'name': 'SHIP4946367_0_R2', 'etag': 'CKqJ2/j80uECEBA=', 'timeCreatedIso': '2019-04-15T20:50:56.112000+00:00', 'matePair': 2, 'updated': '2019-05-03T21:50:19.608Z'}
                        ], 
                        'metadata_setSize': 9
            }, 
            'trellis-metadata': {'sent-from': 'db-query'}}
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}

    result = launch_fastq_to_ubam(event, context=None)