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

    #resource_type = 'nodes'
    #if data['resource'] != resource_type:
    #    print(f"Error: Expected resource type '{resource_type}', " +
    #          f"got '{data['resource']}.'")
    #    return

    nodes = data['results']

    if len(nodes) != 2:
        print(f"Error: Need 2 fastqs; {len(nodes)} provided.")
        return
    #if not 'Fast' in node['labels']:
    #    print(f"Info: Not a Bam object. Ignoring. {node}.")
    #    return

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
        "--min-cores", 1, 
        "--min-ram", 7.5,
        "--preemptible",
        "--boot-disk-size", 20, 
        "--image", 
            f"gcr.io/{PROJECT_ID}/***REMOVED***/wdl_runner:latest", 
        "--logging", 
            f"gs://{LOG_BUCKET}/{sample}/{workflow_name}/{task_name}/logs",
        "--disk-size", "1000",
        "--env", f"RG={read_group}",
        "--env", f"SM={sample}",
        "--env",  "PL=illumina",
        "--command", '/gatk/gatk --java-options "-Xmx8G -Djava.io.tmpdir=bla" FastqToSam -F1 ${FASTQ_1} -F2 ${FASTQ_2} -O ${UBAM} -RG ${RG} -SM ${SM} -PL ${PL}',
        "--output", 
            #f"UBAM=gs://{OUT_BUCKET}/{OUT_ROOT}/{task_group}/{task_name}/{sample}/objects/{sample}_{index}.ubam", 
            f"UBAM=gs://{OUT_BUCKET}/{sample}/{workflow_name}/{task_name}/objects/{sample}_{index}.ubam",
    ]
    ]
    # Add fastqs as inputs
    for name, path in fastqs.items():
        dsub_args.extend(["--input", f"{name}={path}"])

    print(f"Launching dsub with args: {dsub_args}.")
    launch_dsub_task(dsub_args)

# For local testing
if __name__ == "__main__":
    PROJECT_ID = "***REMOVED***-dev"
    ZONES =  "us-west1*"
    OUT_BUCKET = "***REMOVED***-dev-from-personalis-qc"
    OUT_ROOT = "dsub"

    data = {'resource': 'query-result', 'query': 'MATCH (n:Fastq) WHERE n.sample="SHIP4946367" WITH n.readGroup AS read_group, collect(n) AS nodes WHERE size(nodes) = 2 RETURN [n in nodes] AS nodes', 'result': {'nodes': [(_151:Blob:Fastq:WGS_35000 {basename: 'SHIP4946367_0_R1.fastq.gz', bucket: '***REMOVED***-dev-from-personalis', componentCount: 32, contentType: 'application/octet-stream', crc32c: 'ftNG8w==', dirname: 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ', etag: 'CL3nyPj80uECEC4=', extension: 'fastq.gz', generation: '1555361455813565', id: '***REMOVED***-dev-from-personalis/va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565', kind: 'storage#object', labels: ['Fastq', 'WGS_35000', 'Blob'], matePair: 1, mediaLink: 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media', metageneration: '46', name: 'SHIP4946367_0_R1', path: 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz', readGroup: 0, sample: 'SHIP4946367', selfLink: 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz', setSize: 8, size: 5955984357, storageClass: 'REGIONAL', timeCreated: '2019-04-15T20:50:55.813Z', timeCreatedEpoch: 1555361455.813, timeCreatedIso: '2019-04-15T20:50:55.813000+00:00', timeStorageClassUpdated: '2019-04-15T20:50:55.813Z', timeUpdatedEpoch: 1556919952.482, timeUpdatedIso: '2019-05-03T21:45:52.482000+00:00', updated: '2019-05-03T21:45:52.482Z'}), (_242:Blob:Fastq:WGS_35000 {basename: 'SHIP4946367_0_R2.fastq.gz', bucket: '***REMOVED***-dev-from-personalis', componentCount: 32, contentType: 'application/octet-stream', crc32c: 'aV17ew==', dirname: 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ', etag: 'CKqJ2/j80uECEBA=', extension: 'fastq.gz', generation: '1555361456112810', id: '***REMOVED***-dev-from-personalis/va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz/1555361456112810', kind: 'storage#object', labels: ['Fastq', 'WGS_35000', 'Blob'], matePair: 2, mediaLink: 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz?generation=1555361456112810&alt=media', metageneration: '16', name: 'SHIP4946367_0_R2', path: 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz', readGroup: 0, sample: 'SHIP4946367', selfLink: 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz', setSize: 8, size: 6141826914, storageClass: 'REGIONAL', timeCreated: '2019-04-15T20:50:56.112Z', timeCreatedEpoch: 1555361456.112, timeCreatedIso: '2019-04-15T20:50:56.112000+00:00', timeStorageClassUpdated: '2019-04-15T20:50:56.112Z', timeUpdatedEpoch: 1556920219.608, timeUpdatedIso: '2019-05-03T21:50:19.608000+00:00', updated: '2019-05-03T21:50:19.608Z'})]}, 'trellis-metadata': {'sent-from': 'db-query'}}
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}

    result = launch_fastq_to_ubam(event, context=None)
