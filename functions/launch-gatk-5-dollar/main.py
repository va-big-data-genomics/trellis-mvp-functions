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
    LOG_BUCKET = parsed_vars['DSUB_LOG_BUCKET']
    DSUB_USER = parsed_vars['DSUB_USER']
    TRELLIS_BUCKET = parsed_vars['TRELLIS_BUCKET']
    GATK_INPUTS_PATH = parsed_vars['GATK_HG38_INPUTS']

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

def parse_case_results(results):
    #'results': [{'CASE WHEN ': [{node_metadata}]}]
    results = results[0]
    return list(results.values())[0]

def launch_gatk_5_dollar(event, context):
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

    metadata = {}
    nodes = parse_case_results(data['results'])
    #for result_name in data['results']:
    #    elements = result_name.split('_')
    #    if elements[0] == 'metadata':
    #        key = elements[1]
    #        metadata[key] = data['results'][result_name]

    #if len(nodes) != 2:
    #    print(f"Error: Need 2 fastqs; {len(nodes)} provided.")
    #    return

    # Dsub data
    task_name = 'gatk-5-dollar'
    workflow_name = 'fastq-to-vcf'

    # TODO: Implement QC checking to make sure fastqs match
    ubams = []
    for node in nodes:
        if 'Ubam' not in node['labels']:
            print(f"Error: inputs must be ubams.")
            return

        sample = node['sample']
        read_group = node['readGroup']

        bucket = node['bucket']
        path = node['path']

        ubam_path = f"gs://{bucket}/{path}"
        ubams.append(ubam_path)

    datestamp = get_datestamp()

    # Load inputs JSON from GCS
    gatk_input_template = storage.Client() \
        .get_bucket(TRELLIS_BUCKET) \
        .blob(GATK_INPUTS_PATH) \
        .download_as_string()
    gatk_inputs = json.loads(gatk_input_template)

    # Add key/values
    gatk_inputs['germline_single_sample_workflow.sample_name'] = sample
    gatk_inputs['germline_single_sample_workflow.sample_name'] = sample
    gatk_inputs['germline_single_sample_workflow.flowcell_unmapped_bams'] = ubams
    gatk_inputs['germline_single_sample_workflow.final_vcf_base_name'] = sample

    # Write JSON to GCS
    gatk_inputs_path = "{sample}/{workflow_name/{task_name}/inputs/inputs.json"
    gatk_inputs_blob = storage.Client(project=PROJECT_ID) \
        .get_bucket(OUT_BUCKET) \
        .blob(gatk_inputs_path) \
        .upload_from_string(json.dumps(gatk_inputs))
    print(f"Created input blob at gs://{OUT_BUCKET}/{gatk_inputs_path}.")

    dsub_args = [
        "--provider", "google-v2", 
        "--user", DSUB_USER, 
        "--zones", ZONES, 
        "--project", PROJECT_ID,
        "--min-cores", "1", 
        "--min-ram", "6.5",
        "--preemptible",
        "--boot-disk-size", "20", 
        "--image", 
            f"gcr.io/{PROJECT_ID}/***REMOVED***/wdl_runner:latest", 
        "--logging", 
            f"gs://{LOG_BUCKET}/{sample}/{workflow_name}/{task_name}/logs",
        "--disk-size", "1000",
        "--input", f"CFG=gs://{TRELLIS_BUCKET}/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf",
        "--input", f"OPTION=gs://{TRELLIS_BUCKET}/workflow-inputs/gatk-mvp-pipeline/generic.google-papi.options.json",
        "--input", f"WDL=gs://{TRELLIS_BUCKET}/workflow-inputs/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl",
        "--input", f"SUBWDL=gs://{TRELLIS_BUCKET}/workflow-inputs/gatk-mvp-pipeline/tasks_pipeline/*.wdl",
        "--input", f"INPUT=gs://{OUT_BUCKET}/{gatk_inputs_path}",
        "--env", f"MYproject={PROJECT_ID}",
        "--env", f"ROOT=gs://{OUT_BUCKET}/{sample}/{workflow_name}/{task_name}/output",
        "--command", "java -Dconfig.file=${CFG} -Dbackend.providers.JES.config.project=${MYproject} -Dbackend.providers.JES.config.root=${ROOT} -jar /cromwell/cromwell.jar run ${WDL} --inputs ${INPUT} --options ${OPTION}",
    ]

    # Add fastqs as inputs
    #for ubam in ubams:
    #    dsub_args.extend(["--input", f"{name}={path}"])

    print(f"Launching dsub with args: {dsub_args}.")
    result = launch_dsub_task(dsub_args)
    print(f"Dsub result: '{result}'.")

    #if result == 1 and metadata:
    #    print(f"Metadata passed to output blobs: {metadata}.")
    #    # Dump metadata into GCS blob
    #    meta_blob_path = f"{sample}/{workflow_name}/{task_name}/metadata/all-objects.json"
    #    meta_blob = storage.Client(project=PROJECT_ID) \
    #        .get_bucket(OUT_BUCKET) \
    #        .blob(meta_blob_path) \
    #        .upload_from_string(json.dumps(metadata))
    #    print(f"Created metadata blob at gs://{OUT_BUCKET}/{meta_blob_path}.")

# For local testing
if __name__ == "__main__":
    PROJECT_ID = "***REMOVED***-dev"
    ZONES =  "us-west1*"
    OUT_BUCKET = "***REMOVED***-dev-from-personalis-gatk"
    LOG_BUCKET = "***REMOVED***-dev-from-personalis-gatk-logs"
    DSUB_USER = "trellis"

    data = {
            'resource': 'query-result', 
            'query': 'MATCH (n:Fastq) WHERE n.sample="SHIP4946367" WITH n.readGroup AS read_group, collect(n) AS nodes WHERE size(nodes) = 2 RETURN [n in nodes] AS nodes', 
            'results': {
                        'nodes': [
                                  {'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz', 'storageClass': 'REGIONAL', 'setSize': 9, 'timeCreatedEpoch': 1555361455.813, 'timeUpdatedEpoch': 1556919952.482, 'timeCreated': '2019-04-15T20:50:55.813Z', 'id': '***REMOVED***-dev-from-personalis/va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565', 'contentType': 'application/octet-stream', 'generation': '1555361455813565', 'metageneration': '46', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T21:45:52.482000+00:00', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media', 'bucket': '***REMOVED***-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R1.fastq.gz', 'crc32c': 'ftNG8w==', 'size': 5955984357, 'timeStorageClassUpdated': '2019-04-15T20:50:55.813Z', 'name': 'SHIP4946367_0_R1', 'etag': 'CL3nyPj80uECEC4=', 'timeCreatedIso': '2019-04-15T20:50:55.813000+00:00', 'matePair': 1, 'updated': '2019-05-03T21:45:52.482Z'}, 
                                  {'extension': 'fastq.gz', 'readGroup': 0, 'dirname': 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ', 'path': 'va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz', 'storageClass': 'REGIONAL', 'setSize': 9, 'timeCreatedEpoch': 1555361456.112, 'timeUpdatedEpoch': 1556920219.608, 'timeCreated': '2019-04-15T20:50:56.112Z', 'id': '***REMOVED***-dev-from-personalis/va_mvp_phase2/***REMOVED***/SHIP4946367/FASTQ/SHIP4946367_0_R2.fastq.gz/1555361456112810', 'contentType': 'application/octet-stream', 'generation': '1555361456112810', 'metageneration': '16', 'kind': 'storage#object', 'timeUpdatedIso': '2019-05-03T21:50:19.608000+00:00', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz', 'labels': ['Fastq', 'WGS_35000', 'Blob'], 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2F***REMOVED***%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R2.fastq.gz?generation=1555361456112810&alt=media', 'bucket': '***REMOVED***-dev-from-personalis', 'componentCount': 32, 'basename': 'SHIP4946367_0_R2.fastq.gz', 'crc32c': 'aV17ew==', 'size': 6141826914, 'timeStorageClassUpdated': '2019-04-15T20:50:56.112Z', 'name': 'SHIP4946367_0_R2', 'etag': 'CKqJ2/j80uECEBA=', 'timeCreatedIso': '2019-04-15T20:50:56.112000+00:00', 'matePair': 2, 'updated': '2019-05-03T21:50:19.608Z'}
                        ], 
                        'metadata_setSize': 9
            }, 
            'trellis-metadata': {'sent-from': 'db-query'}}
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}

    result = launch_fastq_to_ubam(event, context=None)