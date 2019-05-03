import os
import json
import yaml
import base64

from py2neo import Graph

from google.cloud import pubsub
from google.cloud import storage

# Get runtime variables from cloud storage bucket
# https://www.sethvargo.com/secrets-in-serverless/
ENVIRONMENT = os.environ.get('ENVIRONMENT')
if ENVIRONMENT == 'google-cloud':
    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    # Runtime variables
    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    NEO4J_URL = parsed_vars['NEO4J_URL']
    NEO4J_USER = parsed_vars['NEO4J_USER']
    NEO4J_PASSPHRASE = parsed_vars['NEO4J_PASSPHRASE']

    # Pubsub client
    PUBLISHER = pubsub.PublisherClient()

    # Neo4j graph
    GRAPH = Graph(
                  NEO4J_URL, 
                  user=NEO4J_USER, 
                  password=NEO4J_PASSPHRASE)


def publish_to_topic(publish_topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, publish_topic)
    data = json.dumps(data).encode('utf-8')
    PUBLISHER.publish(topic_path, data=data)


def query_db(event, context):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    #context = base64.b64decode(context).decode('uft-8')
    data = json.loads(pubsub_message)
    print(f"> Context: {context}.")
    print(f"> Data: {data}.")

    # Check that resource is query
    if data['resource'] != 'query':
        print(f"Error: Expected resource type 'request', " +
              f"got '{data['resource']}.'")
        return
    
    neo4j_metadata = data['neo4j-metadata']
    query = neo4j_metadata['cypher']
    result_mode = neo4j_metadata.get('result-mode')
    
    trellis_metadata = data['trellis-metadata']
    topic = trellis_metadata.get('publish-topic')
    result_structure = trellis_metadata.get('result-structure')
    result_split = trellis_metadata.get('result-split')
    
    #### RESTRUCTURED
    if result_mode == 'stats':
        print(f"> Running stats query: {query}.")
        results = GRAPH.run(query).stats()
    elif result_mode == 'data':
        print(f"> Running data query: {query}.")
        results = GRAPH.run(query).data()
    else:
        GRAPH.run(query)
        results = None

    # Return if not pubsub topic
    if not topic:
        return results

    if result_split == 'True':
        for result in results:
            message = {
                    "resource": "query-result",
                    "query": query,
                    "result": result,
            }
            publish_to_topic(topic, message)
            print(f"> Published following message to {topic}: {message}.")
    else:
        message = {
            "resource": "query-result",
            "query": query,
            "result": results,
        }
        publish_to_topic(topic, message)
        print(f"> Published following message to {topic}: {message}.")


if __name__ == "__main__": 
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"
    NEO4J_URL = "https://35.247.31.130:7473"
    NEO4J_USER = "neo4j"
    NEO4J_PASSPHRASE = "IxH3JD_LNPBQq398xSrPifatw7Ha_SSX"

    GRAPH = Graph(
                  NEO4J_URL, 
                  user=NEO4J_USER, 
                  password=NEO4J_PASSPHRASE)

    expected = {
                'path': 'va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz',
                'sample': 'SHIP4946367'
    }

    # Pubsub client
    PUBLISHER = pubsub.PublisherClient()

    try:
        # Create blob node
        data = {
                "resource": "query", 
                "neo4j-metadata": {
                                   "cypher": 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "gbsc-gcp-project-mvp-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECEBE=", generation: "1555361455813565", id: "gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "17", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-04-23T19:17:53.205Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556047073.205, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-04-23T19:17:53.205000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, index: 0}) RETURN node',
                                   "result-mode": "data",
                },
                "trellis-metadata": {"result-resource": "node"}
        }
        data = json.dumps(data).encode('utf-8')
        event = {'data': base64.b64encode(data)}
        result = query_db(event, context=None)

        node = result[0]['node']
        assert len(node.keys()) == 29
        assert node['path'] == expected['path']
        assert node['sample'] == expected['sample']
        print("> Blob node creation test: Pass")
    except:
        print(f"! Error: blob node did not match expected values. {node}.")

    try:
        # Create blob node without trellis-metadata
        data = {
                "resource": "query", 
                "neo4j-metadata": {
                                   "cypher": 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "gbsc-gcp-project-mvp-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECEBE=", generation: "1555361455813565", id: "gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "17", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-04-23T19:17:53.205Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556047073.205, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-04-23T19:17:53.205000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, index: 0}) RETURN node',
                                   "result": "data",
                },
        }
        data = json.dumps(data).encode('utf-8')
        event = {'data': base64.b64encode(data)}
        result = query_db(event, context=None)

        node = result[0]['node']
        assert len(node.keys()) == 29
        assert node['path'] == expected['path']
        assert node['sample'] == expected['sample']
        print("> No trellis metadata test: Pass")
    except:
        print(f"! Error: blob node did not match expected values. {node}.")

    try:
        # Create blob node no trellis-metadata
        data = {
                "resource": "query", 
                "neo4j-metadata": {
                                   "cypher": 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "gbsc-gcp-project-mvp-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECEBE=", generation: "1555361455813565", id: "gbsc-gcp-project-mvp-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "17", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-04-23T19:17:53.205Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556047073.205, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-04-23T19:17:53.205000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, index: 0}) RETURN node',
                },
        }
        data = json.dumps(data).encode('utf-8')
        event = {'data': base64.b64encode(data)}
        result = query_db(event, context=None)
        assert result == None
        print("> No result test: Pass")
    except:
        print(f"! Error: blob node did not match expected values. {node}.")

    # Query fastqs and add set property
    data = {
            'resource': 'query', 
            'neo4j-metadata': {
                               'cypher': 'MATCH (n:Fastq) WHERE n.sample="SHIP4946367" WITH n.sample AS sample, COLLECT(n) AS nodes UNWIND nodes AS node SET node.setSize = size(nodes)RETURN DISTINCT node.setSize AS `added_setSize`, node.sample AS `nodes_sample`, node.labels AS `nodes_labels`', 
                               'result-mode': 'data'
            }, 
            'trellis-metadata': {
                                 'publish-topic': 'wgs35-property-updates', 
                                 'result-structure': 'list', 
                                 'result-split': 'True'
            }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    result = query_db(event, context=None)
