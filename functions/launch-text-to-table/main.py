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

class FastqcTask:

    def __init__(self, node):

        chromosome = node['chromosome']

        self.name = 'text-to-table'
        self.group = 'fastqc-bam' 
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

def launch_text_to_table(event, context):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)

    # Check that resource is blob
    resource_type = 'node'
    if data['resource'] != resource_type:
        print(f"Error: Expected resource type '{resource_type}', " +
              f"got '{data['resource']}.'")
        return

    node = data['neo4j-metadata']['node']
    labels = node['labels']

    # Check whether blob label is supported
    supported_labels = {
                       'Fastqc_data': FastqcTask, 
                       'Flagstat_data': FlagstatTask, 
                       'Vcfstats_data': VcfstatsTask
    }
    
    task_labels = set(supported_labels.keys()).intersection(set(labels))
    if not task_labels:
        print(f"Info: Not a supported object. Ignoring {node}.")
        return 
    elif len(task_labels) > 1:
        print(f"Error: Blob matches multiple labels. Ignoring {node}.")
        return
    label = task_labels.pop()

    # Get task-specific metadata
    task = supported_labels[label](node)
    task_name = task.name
    task_group = task.group
    schema_name = task.schema_name
    series = task.series
    json_schema = task.json_schema

    # Get task-generic metadata
    datestamp = get_datestamp()

    in_bucket = node['bucket']
    in_path = node['path']
    
    sample = node['sample']
    basename = node['basename']

    dsub_args = [
        "--provider", "google-v2",
        "--user", DSUB_USER,
        "--zones", zones, 
        "--project", project_id, 
        "--logging", 
            f"gs://{log_bucket}/{out_root}/{task_group}/{task_name}/logs/{datestamp}",
        "--image", 
            f"gcr.io/{project_id}/text-to-table:0.2.1", 
        "--command", "text2table -j ${JSON} -o ${OUTPUT} -v series=${SERIES},sample=${SAMPLE_ID} ${INPUT}",
        "--input", 
            f"JSON=gs://{out_bucket}/schemas/{json_schema}",
        "--input", 
            f"INPUT=gs://{in_bucket}/{in_path}",
        "--output", 
            f"OUTPUT=gs://{out_bucket}/{out_root}/{task_group}/{task_name}/objects/{basename}.csv",
        "--env", f"SAMPLE_ID={sample}", 
        "--env", f"SCHEMA={schema_name}", 
        "--env", f"SERIES={series}"
    ]
    print(dsub_args)
    launch_dsub_task(dsub_args)

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
