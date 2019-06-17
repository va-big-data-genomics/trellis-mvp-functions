import os
import pdb
import sys
import json
import pytz
import yaml
import base64
import iso8601
import importlib

from datetime import datetime

from google.cloud import pubsub
from google.cloud import storage

# Environment variables
ENVIRONMENT = os.environ.get('ENVIRONMENT')
if ENVIRONMENT == 'google-cloud':
    FUNCTION_NAME = os.environ['FUNCTION_NAME']

    vars_blob = storage.Client() \
            .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
            .get_blob(os.environ['CREDENTIALS_BLOB']) \
            .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    QUERY_TOPIC = parsed_vars['DB_QUERY_TOPIC']
    NODE_TRIGGERS_TOPIC = parsed_vars['NODE_TRIGGERS_TOPIC']
    DATA_GROUP = parsed_vars['DATA_GROUP']

    # Establish PubSub connection
    PUBLISHER = pubsub.PublisherClient()


def format_pubsub_message(query, topic=None):
    message = {
               "header": {
                          "resource": "query", 
                          "method": "POST",
                          "labels": ["Cypher", "Query", "Relationship", "Create"],
                          "sentFrom": FUNCTION_NAME,
                          "publishTo": topic,
               },
               "body": {
                        "cypher": query,
                        "result-mode": "data",
                        "result-structure": "list",
                        "result-split": "True", 
               }
    }
    return message


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result


def add_relationships(event, context):
    """Get a message with a node & relationships field with
       relationship metadata. Add these relationships to the database.


       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    print(f"Data: {data}.\n")
    header = data['header']
    body = data['body']

    if header['resource'] != 'queryResult':
        print(f"Error: Expected resource type 'blob', " +
              f"got '{header['resource']}.'")
        return

    node = body['results']['node']

    # If node has 'bucket' property; get relationship rules
    # Import the config modules that corresponds to event-trigger bucket
    # Logic for new blobs
    result = None
    bucket_name = node.get('bucket')
    if bucket_name:
        config_module_name = f"{DATA_GROUP}.{bucket_name}.create-node-config"
        config_module = importlib.import_module(config_module_name)

        # Add provided relationships
        relationship_rules = config_module.RelationshipKinds()
        shipping_properties = relationship_rules.shipping_properties

        # Generate relationship queries for node property triggers
        ship_queries = []
        for property_name, functions in shipping_properties.items():
            if property_name in node.keys():
                for function in functions:
                    ship_query = function(node)
                    ship_queries.append(ship_query)

        for query in ship_queries:
            message = format_pubsub_message(query, NODE_TRIGGERS_TOPIC)
            result = publish_to_topic(QUERY_TOPIC, message)
            print(
                  f"> Published following message to {QUERY_TOPIC} with " + 
                  f"result {result}: {message}.")

    # For nodes that do not have bucket property (i.e. jobs), 
    #   relationships must be specified manually

    # Create additional relationships written directly to message
    relationships = body.get('relationships')
    # If no relationships included, push message to node triggers
    if not relationships and not result:
        print(f"> No relationships, pushing data to node triggers.")
        print(f"> Pubsub message: {data}.")
        result = publish_to_topic(NODE_TRIGGERS_TOPIC, data)
        print(f"> Published message to {NODE_TRIGGERS_TOPIC} with result: {result}.")
        return

    # Write a generic relationship query
    # Logic for new jobs
    if not relationships:
        return
    for orientation in relationships:
        for relationship_name in relationships[orientation]:
            for related_node in relationships[orientation][relationship_name]:
                
                # Create property string for related node
                related_strings = []
                for key, value in related_node.items():
                    if isinstance(value, str):
                        related_strings.append(f'{key}: "{value}"')
                    else:
                        related_strings.append(f'{key}: {value}')
                related_string = ', '.join(related_strings)

                # Create property string for input node
                node_strings = []
                for key, value in node.items():
                    if isinstance(value, str):
                        node_strings.append(f'{key}: "{value}"')
                    else:
                        node_strings.append(f'{key}: {value}')
                node_string = ', '.join(node_strings)
                
                if orientation == "to-node":
                    query = f"""
                            MATCH (related_node {{ {related_string} }}), 
                                  (node {{ {node_string} }})
                            CREATE (related_node)-[:{relationship_name}]->(node)
                            """
                elif orientation == "from-node":
                    query = f"""
                            MATCH (related_node {{ {related_string} }}), 
                                  (node {{ {node_string} }})
                            CREATE (node)-[:{relationship_name}]->(related_node)
                            """
                elif orientation == "bidirectional":
                    query = f"""
                            MATCH (related_node {{ {related_string} }}), 
                                  (node {{ {node_string} }})
                            CREATE (node)-[:{relationship_name}]-(related_node)
                            """

                message = format_pubsub_message(query, None)
                result = publish_to_topic(QUERY_TOPIC, message)
                print(
                      f"> Published following message to {QUERY_TOPIC} with " + 
                      f"result {result}: {message}.")


# For local testing
if __name__ == "__main__":
    PROJECT_ID = "***REMOVED***-dev"
    QUERY_TOPIC = "wgs35-db-queries"
    NODE_TRIGGERS_TOPIC = "wgs35-node-triggers"
    FUNCTION_NAME = "add-relationships"
    DATA_GROUP = "wgs35"

    PUBLISHER = pubsub.PublisherClient()

    data = b'{"header": {"method": "VIEW", "resource": "query-result", "labels": ["Cypher", "Query", "Result"]}, "body": {"query": "CREATE (node:Job:Cromwell {provider: \\"google-v2\\", user: \\"trellis\\", zones: \\"us-west1*\\", project: \\"***REMOVED***-dev\\", minCores: 1, minRam: 6.5, preemptible: True, bootDiskSize: 20, image: \\"gcr.io/***REMOVED***-dev/***REMOVED***/wdl_runner:latest\\", logging: \\"gs://***REMOVED***-dev-from-personalis-gatk-logs/SHIP4946367/fastq-to-vcf/gatk-5-dollar/logs\\", diskSize: 1000, command: \\"java -Dconfig.file=${CFG} -Dbackend.providers.JES.config.project=${MYproject} -Dbackend.providers.JES.config.root=${ROOT} -jar /cromwell/cromwell.jar run ${WDL} --inputs ${INPUT} --options ${OPTION}\\", dryRun: True, labels: [\'Job\', \'Cromwell\'], input_CFG: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf\\", input_OPTION: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json\\", input_WDL: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl\\", input_SUBWDL: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl\\", input_INPUT: \\"gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json\\", env_MYproject: \\"***REMOVED***-dev\\", env_ROOT: \\"gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output\\", timeCreatedEpoch: 1559080699.59893, timeCreatedIso: \\"2019-05-28T21:58:19.598930+00:00\\"}) RETURN node", "sent-from": "wgs35-db-query", "relationships": {"to-node": {"INPUT_TO": [{"basename": "SHIP4946367_2.ubam", "bucket": "***REMOVED***-dev-from-personalis-gatk", "contentType": "application/octet-stream", "crc32c": "ojStVg==", "dirname": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output", "etag": "CJTpxe3ynuICEAM=", "extension": "ubam", "generation": "1557970088457364", "id": "***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam/1557970088457364", "kind": "storage#object", "labels": ["WGS35", "Blob", "Ubam"], "md5Hash": "opGAi0f9olAu4DKzvYiayg==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam?generation=1557970088457364&alt=media", "metageneration": "3", "name": "SHIP4946367_2", "path": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam", "sample": "SHIP4946367", "selfLink": "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam", "size": 16886179620, "storageClass": "REGIONAL", "timeCreated": "2019-05-16T01:28:08.455Z", "timeCreatedEpoch": 1557970088.455, "timeCreatedIso": "2019-05-16T01:28:08.455000+00:00", "timeStorageClassUpdated": "2019-05-16T01:28:08.455Z", "timeUpdatedEpoch": 1558045261.522, "timeUpdatedIso": "2019-05-16T22:21:01.522000+00:00", "trellisTask": "fastq-to-ubam", "trellisWorkflow": "fastq-to-vcf", "updated": "2019-05-16T22:21:01.522Z"}, {"basename": "SHIP4946367_0.ubam", "bucket": "***REMOVED***-dev-from-personalis-gatk", "contentType": "application/octet-stream", "crc32c": "ZaJM+g==", "dirname": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output", "etag": "CM+sxKDynuICEAY=", "extension": "ubam", "generation": "1557969926952527", "id": "***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam/1557969926952527", "kind": "storage#object", "labels": ["WGS35", "Blob", "Ubam"], "md5Hash": "Tgh+eyIiKe8TRWV6vohGJQ==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam?generation=1557969926952527&alt=media", "metageneration": "6", "name": "SHIP4946367_0", "path": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam", "sample": "SHIP4946367", "selfLink": "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam", "size": 16871102587, "storageClass": "REGIONAL", "timeCreated": "2019-05-16T01:25:26.952Z", "timeCreatedEpoch": 1557969926.952, "timeCreatedIso": "2019-05-16T01:25:26.952000+00:00", "timeStorageClassUpdated": "2019-05-16T01:25:26.952Z", "timeUpdatedEpoch": 1558045265.901, "timeUpdatedIso": "2019-05-16T22:21:05.901000+00:00", "trellisTask": "fastq-to-ubam", "trellisWorkflow": "fastq-to-vcf", "updated": "2019-05-16T22:21:05.901Z"}]}}, "results": [{"node": {"image": "gcr.io/***REMOVED***-dev/***REMOVED***/wdl_runner:latest", "dryRun": true, "minCores": 1, "input_SUBWDL": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl", "input_WDL": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl", "project": "***REMOVED***-dev", "zones": "us-west1*", "input_CFG": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf", "command": "java -Dconfig.file=${CFG} -Dbackend.providers.JES.config.project=${MYproject} -Dbackend.providers.JES.config.root=${ROOT} -jar /cromwell/cromwell.jar run ${WDL} --inputs ${INPUT} --options ${OPTION}", "labels": ["Job", "Cromwell"], "diskSize": 1000, "preemptible": true, "provider": "google-v2", "input_OPTION": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json", "env_ROOT": "gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output", "timeCreatedEpoch": 1559080699.59893, "minRam": 6.5, "logging": "gs://***REMOVED***-dev-from-personalis-gatk-logs/SHIP4946367/fastq-to-vcf/gatk-5-dollar/logs", "timeCreatedIso": "2019-05-28T21:58:19.598930+00:00", "env_MYproject": "***REMOVED***-dev", "input_INPUT": "gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json", "bootDiskSize": 20, "user": "trellis"}}]}}'
    #data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}

    add_relationships(event, context=None)
