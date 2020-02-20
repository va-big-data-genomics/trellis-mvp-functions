import os
import sys
import json
import base64

from datetime import datetime

from google.cloud import bigquery
from google.cloud import exceptions

PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT', '')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', '')

CLIENT = bigquery.Client(project=PROJECT_ID)

class BigQueryTable:

    def __init__(self, name, csv_uri, schema):
        """

        Args:
            name (str): Bigquery table config data
            csv_uri (str): Link to CSV source file
            schema_fields (dict): Key = column name & value = type, i.e. "STRING"
        """
        self.name = name
        self.csv_uri = csv_uri
        self.schema_fields = schema

    def write_csv_to_table(self):
        print(f"Loading table: {self.name}.")
        print(f"{BIGQUERY_DATASET} {PROJECT_ID}.")
        
        dataset_ref = CLIENT.dataset(BIGQUERY_DATASET)
        
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV 
        job_config.write_disposition = 'WRITE_TRUNCATE'

        bq_schema = []
        for label, kind in self.schema_fields.items():
            bq_schema.append(bigquery.SchemaField(label, kind))
        job_config.schema = bq_schema
        print(f"Table schema: {job_config.schema}")

        print(f"Job configuration: {job_config}.")
        load_job = CLIENT.load_table_from_uri(
                                    source_uris = self.csv_uri, 
                                    destination = dataset_ref.table(self.name), 
                                    job_config = job_config)
        print(f"Starting job {load_job.job_id}.")

        destination_table = CLIENT.get_table(dataset_ref.table(self.name))
        print(f"Loaded {destination_table.num_rows} rows.")

def import_csv_to_bigquery(event, context):
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
    
    if not 'Concat' in node['labels']:
        #print(f"Info: Not a Concat object. Ignoring {node}.") # Too noisy
        return 

    # Check whether node label is supported
    supported_labels = [
                       'Fastqc',
                       'Flagstat', 
                       'Vcfstats']

    task_labels = set(supported_labels).intersection(set(node_labels))
    if not task_labels:
        print(f"Info: These labels are not supported. Ignoring. {node}.")
        return 
    elif len(task_labels) > 1:
        print(f"Error: Request matches multiple labels. Ignoring. {node}.")
        return
    label = task_labels.pop()
    print(f"Node matches import task for {label}.")

    # Get BigQuery table configurations
    with open('lib/bigquery-config.json') as fh:
        bigquery_configs = json.load(fh)
    config_data = bigquery_configs[label]

    csv_bucket = node['bucket']
    csv_path = node['path']
    csv_uri = f"gs://{csv_bucket}/{csv_path}"
    
    # I don't know why I have this in a separate function...
    print(f"Loading object {csv_uri} into {config_data['table-name']} in {BIGQUERY_DATASET}.")
    bq_table = BigQueryTable(
                             name = config_data['table-name'], 
                             csv_uri = csv_uri,
                             schema = config_data['schema-fields'])
    try:
        bq_table.write_csv_to_table()
    except excpetions.NotFound:
        print(f"Table not found. Creating table.")
        dataset_ref = CLIENT.dataset(BIGQUERY_DATASET)
        CLIENT.create_table(bq_table)
        tables = list(CLIENT.list_tables(dataest_ref))
        print(f"Table created. Dataset tables: {tables}.")

# For local testing
if __name__ == "__main__":
    PROJECT_ID = "***REMOVED***-dev"
    BIGQUERY_DATASET = 'mvp_wgs_9000'
    CLIENT = bigquery.Client(project=PROJECT_ID)

    data = {
            'resource': 'node', 
            'neo4j-metadata': {
                               'node': {
                                        'extension': 'txt.csv', 
                                        'time-created-iso': '2019-04-02T23:09:30.066000+00:00', 
                                        'dirname': 'dsub/vcfstats/concat/objects', 
                                        'path': 'dsub/vcfstats/concat/objects/concat_vcfstats.txt.csv', 
                                        'storageClass': 'REGIONAL', 
                                        'md5Hash': '2lSvb8PhCk/nssCM3R2y+A==', 
                                        'timeCreated': '2019-04-02T23:09:30.066Z', 
                                        'time-updated-iso': '2019-04-02T23:09:30.066000+00:00', 
                                        'id': '***REMOVED***-dev-from-personalis-qc/dsub/vcfstats/concat/objects/concat_vcfstats.txt.csv/1554246570068077', 
                                        'contentType': 'text/csv', 
                                        'generation': '1554246570068077', 
                                        'metageneration': '1', 
                                        'kind': 'storage#object', 
                                        'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-qc/o/dsub%2Fvcfstats%2Fconcat%2Fobjects%2Fconcat_vcfstats.txt.csv?generation=1554246570068077&alt=media', 
                                        'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-qc/o/dsub%2Fvcfstats%2Fconcat%2Fobjects%2Fconcat_vcfstats.txt.csv', 
                                        'labels': ['Vcfstats', 'Concat', 'WGS_9000', 'Blob'], 
                                        'bucket': '***REMOVED***-dev-from-personalis-qc', 
                                        'basename': 'concat_vcfstats.txt.csv', 
                                        'crc32c': 'wO1axA==', 
                                        'size': 3177403, 
                                        'timeStorageClassUpdated': '2019-04-02T23:09:30.066Z', 
                                        'task-group': 'vcfstats', 
                                        'name': 'concat_vcfstats', 
                                        'etag': 'CO2Qz9XDsuECEAE=', 
                                        'updated': '2019-04-02T23:09:30.066Z', 
                                        'time-created-epoch': 1554246570.066, 
                                        'time-updated-epoch': 1554246570.066
                               }
            }
    }
    data = json.dumps(data)
    data = data.encode('utf-8')
    
    event = {'data': base64.b64encode(data)}

    import_csv_to_bigquery(event, context=None)
