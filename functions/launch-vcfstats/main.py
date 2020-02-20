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

def launch_vcfstats(event, context):
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

    if not 'Vcf' in node['labels']:
        print(f"Info: Not a VCF object. Ignoring {node}.")
        return 

    # Dsub data
    task_name = 'rtg-tools'
    task_group = 'vcfstats'

    datestamp = get_datestamp()

    # Database entry variables
    in_bucket = node['bucket']
    in_path = node['path']
    sample = node['sample']

    dsub_args = [
        "--provider", "google-v2",
        "--user", DSUB_USER, 
        "--zones", zones, 
        "--project", project_id, 
        "--logging", 
            f"gs://{log_bucket}/{out_root}/{task_group}/{task_name}/logs/{datestamp}",
        "--image", 
            f"gcr.io/{project_id}/rtg-tools:1.0", 
        "--command", "rtg vcfstats ${INPUT} > ${OUTPUT}", 
        "--input", 
            f"INPUT=gs://{in_bucket}/{in_path}",
        "--output", 
            f"OUTPUT=gs://{out_bucket}/{out_root}/{task_group}/{task_name}/objects/{sample}_rtg_vcfstats.txt",
        "--env", f"SAMPLE_ID={sample}"
    ]
    print(f"Launching dsub with args: {dsub_args}.")
    launch_dsub_task(dsub_args)

# For local testing
if __name__ == "__main__":
    project_id = "gbsc-gcp-project-mvp-dev"
    zones =  "us-west1*"
    out_bucket = "gbsc-gcp-project-mvp-dev-from-personalis-qc"
    out_root = "dsub"

    data = {"resource": "blob", "gcp-metadata": {"bucket": "gbsc-gcp-project-mvp-dev-from-personalis", "contentType": "text/vcard", "crc32c": "EMJeaA==", "etag": "CPKaqbS84uACEAI=", "generation": "1551495842123122", "id": "gbsc-gcp-project-mvp-dev-from-personalis/SHIP3935743/Variants/SHIP3935743.snvindel.var.vcf.gz/1551495842123122", "kind": "storage#object", "md5Hash": "a7zCh6W1CLUca9JbkWBrmg==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/SHIP3935743%2FVariants%2FSHIP3935743.snvindel.var.vcf.gz?generation=1551495842123122&alt=media", "metadata": {"test": "20190308"}, "metageneration": "2", "name": "SHIP3935743/Variants/SHIP3935743.snvindel.var.vcf.gz", "selfLink": "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/SHIP3935743%2FVariants%2FSHIP3935743.snvindel.var.vcf.gz", "size": "224959007", "storageClass": "REGIONAL", "timeCreated": "2019-03-02T03:04:02.122Z", "timeStorageClassUpdated": "2019-03-02T03:04:02.122Z", "updated": "2019-03-08T23:59:14.571Z"}, "trellis-metadata": {"path": "SHIP3935743/Variants/SHIP3935743.snvindel.var.vcf.gz", "dirname": "SHIP3935743/Variants", "basename": "SHIP3935743.snvindel.var.vcf.gz", "extension": "snvindel.var.vcf.gz", "time-created-epoch": 1551495842.122, "time-updated-epoch": 1552089554.571, "time-created-iso": "2019-03-02T03:04:02.122000+00:00", "time-updated-iso": "2019-03-08T23:59:14.571000+00:00", "labels": ["Vcf", "WGS_9000", "Blob"], "sample": "SHIP3935743"}}
    data = json.dumps(data)
    data = data.encode('utf-8')
    
    event = {'data': base64.b64encode(data)}

    launch_vcfstats(event, context=None)
