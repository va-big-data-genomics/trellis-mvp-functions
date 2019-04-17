import os
import re
import json
import importlib

from google.cloud import storage
from google.cloud import pubsub

from py2neo import Graph

# Environment variables
NEO4J_URL = os.environ.get('NEO4J_URL')
NEO4J_USER = os.environ.get('NEO4J_USER')
NEO4J_PASSPHRASE = os.environ.get('NEO4J_PASSPHRASE')
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT', '')
TOPIC = os.environ.get('NEW_DB_NODE_TOPIC', '')
DATA_GROUP = os.environ.get('DATA_GROUP', '')

PUBLISHER = pubsub.PublisherClient()
TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                   id = PROJECT_ID, 
                                                   topic = TOPIC)

graph = Graph(
              NEO4J_URL, 
              user=NEO4J_USER, 
              password=NEO4J_PASSPHRASE)

def query_graph(query):
    """Query graph using user-provided queries
    """
    #graph = get_neo4j_graph()
    print(query)
    return graph.run(query).data()

def merge_node_to_graph(db_entry):

    merge_keys = [
                  'bucket', 
                  'path', 
                  'size',
                  'md5Hash']

    # Find the intersection of the db_entry keys and merge properties
    # so that only merging on properties that exist for this db entry
    intersect_keys = set(merge_keys).intersection(db_entry.keys())

    merge_properties = {}
    for key, value in db_entry.items():
        if key in intersect_keys:
            merge_properties[key] = '"{}"'.format(value)

    # Join all merge properties into string to pass in Cypher query
    merge_properties_str = ', '.join(
                                        "{}:{}".format(key, val) 
                                        for (key, val) 
                                        in merge_properties.items())
    # Join all neo4j labels into string
    labels_str = ':'.join(db_entry['labels'])

    query = (
             'MERGE (n:{} '.format(labels_str) +
             '{{ {} }}) '.format(merge_properties_str) +  
             'ON CREATE SET n = {entry}')
    graph.run(query, entry=db_entry)
    print(f"Db entry merged to neo4j database: {db_entry}.")

def create_node_in_graph(db_entry):
    labels_str = ':'.join(db_entry['labels'])

    query = (
             f'CREATE (node:{labels_str} ' + 
                      '{entry}) ' + 
              'RETURN node')
    node = graph.run(query, entry=db_entry).data()[0]
    print(f"Created node in Neo4j database. {node}.")
    return node

def add_obj_to_db(event, context):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    print(f"Processing new object event: {event['name']}.")
    print(f"Event: {event}).")
    print(f"Context: {context}.")

    # Trellis config data
    name = event['name']
    bucket_name = event['bucket']

    # Import the config module that corresponds to event-trigger bucket
    module_name = f"{DATA_GROUP}.{bucket_name}.create-node-config"
    bucket_module = importlib.import_module(module_name)

    node_kinds = bucket_module.NodeKinds()
    label_patterns = node_kinds.get_match_patterns()

    # Determine which kind patterns match the object name
    labels = []
    for key, values in label_patterns.items():
        for pattern in values:
            match = re.fullmatch(pattern, name)
            if match:
                labels.append(key)
                break
    
    if labels:
        global_labels = node_kinds.get_global_labels()
        labels.extend(global_labels)
        print(f"Node labels: {labels}.")
    else:
        print(
              "Warning: no database entry created. " + 
              f"Name '{name}' does not match any label patterns.")
        return

    # Get label-specific metadata functions    
    label_functions = node_kinds.get_label_functions(labels)


    node_obj = bucket_module.DatabaseNode(
                                          event, 
                                          context, 
                                          labels, 
                                          label_functions)
    node_dict = node_obj.get_db_dict()

    print(f"Adding db entry to db: {node_dict}.")
    #merge_node_to_graph(node_dict)         # Merge is slow.
    node = create_node_in_graph(node_dict)

    message = {
               "resource": "node", 
               "neo4j-metadata": node
    }

    message = json.dumps(message).encode('utf-8')
    result = PUBLISHER.publish(TOPIC_PATH, data=message).result()
    print(f"Published message to {TOPIC_PATH}: {result}.")
    #return

    # Execute node triggers (DEV)
    # TODO: Test in dev
    module_name = f"lib.{bucket_name}.trigger-config"
    trigger_module = importlib.import_module(module_name)

    trigger_config = trigger_module.NodeTriggers(
                                                 project_id = PROJECT_ID, 
                                                 node = node)
    triggers = trigger_config.get_triggers()
    print(f"Node triggers: {triggers}.")
    trigger_config.execute_triggers()



if __name__ == "__main__": 
    # dev credentials
    NEO4J_URL = "https://35.247.31.130:7473"
    NEO4J_USER = "neo4j"
    NEO4J_PASSPHRASE = "IxH3JD_LNPBQq398xSrPifatw7Ha_SSX"
    PROJECT_ID = "***REMOVED***-dev"
    TOPIC = "wgs-9000-new-db-node"

    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID, 
                                                       topic = TOPIC)

    graph = Graph(
                  NEO4J_URL, 
                  user=NEO4J_USER, 
                  password=NEO4J_PASSPHRASE)

    """
    event = {
             'bucket': '***REMOVED***-dev-from-personalis-qc', 
             'contentType': 'application/octet-stream', 
             'crc32c': 'AAAAAA==', 
             'etag': 'COya25XMh98CEAE=', 
             'generation': '1543975290195308', 
             'id': '***REMOVED***-dev-from-personalis-qc/dsub/fastqc-bam/fastqc/objects/SHIP3935743_chromosome_10_alignments.bam.fastqc_data.txt', 
             'kind': 'storage#object', 
             'md5Hash': '1B2M2Y8AsgTpgAmY7PhCfg==', 
             'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-group/o/function-test%2FAlignments%2Ftest.bam?generation=1543975290195308&alt=media', 
             'metageneration': '1', 
             'name': 'dsub/fastqc-bam/fastqc/objects/SHIP3935743_chromosome_10_alignments.bam.fastqc_data.txt', 
             'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-group/o/function-test%2FAlignments%2Ftest.bam', 
             'size': '0', 
             'storageClass': 'DURABLE_REDUCED_AVAILABILITY', 
             'timeCreated': '2018-12-05T02:01:30.194Z', 
             'timeStorageClassUpdated': '2018-12-05T02:01:30.194Z', 
             'updated': '2018-12-05T02:01:30.194Z'
    }
    """

    event = {
             'bucket': '***REMOVED***-dev-from-personalis-qc', 
             'contentType': 'text/csv', 
             'crc32c': 'wO1axA==', 
             'etag': 'CO2Qz9XDsuECEAE=', 
             'generation': '1554246570068077', 
             'id': '***REMOVED***-dev-from-personalis-qc/dsub/vcfstats/concat/objects/concat_vcfstats.txt.csv/1554246570068077', 
             'kind': 'storage#object', 
             'md5Hash': '2lSvb8PhCk/nssCM3R2y+A==', 
             'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-qc/o/dsub%2Fvcfstats%2Fconcat%2Fobjects%2Fconcat_vcfstats.txt.csv?generation=1554246570068077&alt=media', 
             'metageneration': '1', 
             'name': 'dsub/vcfstats/concat/objects/concat_vcfstats.txt.csv', 
             'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-qc/o/dsub%2Fvcfstats%2Fconcat%2Fobjects%2Fconcat_vcfstats.txt.csv', 
             'size': '3177403', 
             'storageClass': 'REGIONAL', 
             'timeCreated': '2019-04-02T23:09:30.066Z', 
             'timeStorageClassUpdated': '2019-04-02T23:09:30.066Z', 
             'updated': '2019-04-02T23:09:30.066Z'
    }

    context = None
    add_obj_to_db(event, context)
