import os
import sys
import json
import base64

from datetime import datetime

from dsub.commands import dsub

project_id = os.environ.get('GOOGLE_CLOUD_PROJECT', '')
zones = os.environ.get('ZONES', '')
out_bucket = os.environ.get('DSUB_OUT_BUCKET', '')
log_bucket = os.environ.get('DSUB_LOG_BUCKET', '')
out_root = os.environ.get('DSUB_OUT_ROOT', '')
DSUB_USER = os.environ.get('DSUB_USER', '')

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

def launch_fastqc(event, context):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(data)

    resource_type = 'node'
    if data['resource'] != resource_type:
        print(f"Error: Expected resource type '{resource_type}', " +
              f"got '{data['resource']}.'")
        return

    node = data['neo4j-metadata']['node']

    if not 'Bam' in node['labels']:
        print(f"Info: Not a Bam object. Ignoring. {node}.")
        return 

    # Dsub data
    task_name = 'fastqc'
    task_group = 'fastqc-bam'

    datestamp = get_datestamp()

    # Database entry variables
    in_bucket = node['bucket']
    in_path = node['path']
    
    sample = node['sample']
    basename = node['basename']
    chromosome = node['chromosome']

    dsub_args = [
        "--provider", "google-v2", 
        "--user", DSUB_USER, 
        "--zones", zones, 
        "--project", project_id, 
        "--logging", 
            f"gs://{log_bucket}/{out_root}/{task_group}/{task_name}/logs/{datestamp}",
        "--image", 
            f"gcr.io/{project_id}/fastqc:1.01", 
        "--disk-size", "1000", 
        "--script", "lib/fastqc.sh", 
        "--input", 
            f"INPUT=gs://{in_bucket}/{in_path}",
        "--output", 
            f"OUTPUT=gs://{out_bucket}/{out_root}/{task_group}/{task_name}/objects/{basename}.fastqc_data.txt",
        "--env", f"SAMPLE_ID={sample}"
    ]
    print(f"Launching dsub with args: {dsub_args}.")
    launch_dsub_task(dsub_args)

# For local testing
if __name__ == "__main__":
    project_id = "***REMOVED***-dev"
    zones =  "us-west1*"
    out_bucket = "***REMOVED***-dev-from-personalis-qc"
    out_root = "dsub"

    data = {"resource": "blob", "gcp-metadata": {"bucket": "***REMOVED***-dev-from-personalis", "contentLanguage": "en", "contentType": "application/octet-stream", "crc32c": "XPwmhA==", "etag": "CMTx8Y/t8+ACEAE=", "generation": "1552093034608836", "id": "***REMOVED***-dev-from-personalis/SHIP4420818/Alignments/SHIP4420818_chromosome_Y.recal.bam/1552093034608836", "kind": "storage#object", "md5Hash": "qm+p+AbPhpyXwmXtAQ/fkA==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/SHIP4420818%2FAlignments%2FSHIP4420818_chromosome_Y.recal.bam?generation=1552093034608836&alt=media", "metadata": {"goog-reserved-file-mtime": "1527072114"}, "metageneration": "1", "name": "SHIP4420818/Alignments/SHIP4420818_chromosome_Y.recal.bam", "selfLink": "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/SHIP4420818%2FAlignments%2FSHIP4420818_chromosome_Y.recal.bam", "size": "921001444", "storageClass": "REGIONAL", "timeCreated": "2019-03-09T00:57:14.607Z", "timeStorageClassUpdated": "2019-03-09T00:57:14.607Z", "updated": "2019-03-09T00:57:14.607Z"}, "trellis-metadata": {"path": "SHIP4420818/Alignments/SHIP4420818_chromosome_Y.recal.bam", "dirname": "SHIP4420818/Alignments", "basename": "SHIP4420818_chromosome_Y.recal.bam", "extension": "recal.bam", "time-created-epoch": 1552093034.607, "time-updated-epoch": 1552093034.607, "time-created-iso": "2019-03-09T00:57:14.607000+00:00", "time-updated-iso": "2019-03-09T00:57:14.607000+00:00", "labels": ["Bam", "WGS_9000", "Blob"], "sample": "SHIP4420818", "chromosome": "Y"}}
    data = json.dumps(data)
    data = data.encode('utf-8')
    
    event = {'data': base64.b64encode(data)}

    launch_fastqc(event, context=None)
