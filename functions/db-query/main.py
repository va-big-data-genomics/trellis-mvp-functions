import os
import sys
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
    DATA_GROUP = parsed_vars['DATA_GROUP']
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


def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    PUBLISHER.publish(topic_path, data=message)
    print(f"> Published following message to {topic}: {message}.")


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
    header = data['header']
    body = data['body']

    # Check that resource is query
    if header['resource'] != 'query':
        print(f"Error: Expected resource type 'request', " +
              f"got '{data['resource']}.'")
        return
    
    #neo4j_metadata = data['neo4j-metadata']
    #query = neo4j_metadata['cypher']
    query = body['cypher']
    #result_mode = neo4j_metadata.get('result-mode')
    result_mode = body.get('result-mode')

    #trellis_metadata = data['trellis-metadata']
    #topic = trellis_metadata.get('publish-topic')
    topic = body.get('publish-topic')
    #result_structure = trellis_metadata.get('result-structure')
    #result_split = trellis_metadata.get('result-split')
    result_structure = body.get('result-structure')
    result_split = body.get('result-split')
    
    #### RESTRUCTURED
    if result_mode == 'stats':
        print(f"> Running stats query: '{query}'.")
        results = GRAPH.run(query).stats()
    elif result_mode == 'data':
        print(f"> Running data query: '{query}'.")
        results = GRAPH.run(query).data()
    else:
        GRAPH.run(query)
        results = None
    print(f"Query results: {results}.")

    # Return if not pubsub topic
    if not topic:
        print("No Pub/Sub topic specified; result not published.")
        return results

    message = {
               "header": {
                          "method": "VIEW",
                          "resource": "query-result",
                          "labels": ["Cypher", "Query", "Result"],
               },
               "body": {
                        "query": query, 
                        "sent-from": f"{DATA_GROUP}-db-query",
               }
               #"query": query,
               #"trellis": {"sent-from": "db-query"},
    }

    # Perpetuate metadata in specified by "perpetuate" key
    perpetuate = body.get('perpetuate')
    if perpetuate:
        message['body'].update(perpetuate)

    if result_split == 'True':
        for result in results:
            message['body']['results'] = result
            publish_to_topic(topic, message)
            #print(f"> Published following message to {topic}: {message}.")
    else:
        message['body']['results'] = results
        publish_to_topic(topic, message)
        #print(f"> Published following message to {topic}: {message}.")


if __name__ == "__main__": 
    PROJECT_ID = "***REMOVED***-dev"
    DATA_GROUP = "wgs35"
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

    # Create gatk-5-dollar job node
    data = b'{"header": {"method": "POST", "labels": ["Job", "Create", "Node", "Query", "Cypher"], "resource": "query"}, "body": {"cypher": "CREATE (node:Job:Cromwell {provider: \\"google-v2\\", user: \\"trellis\\", zones: \\"us-west1*\\", project: \\"***REMOVED***-dev\\", minCores: 1, minRam: 6.5, preemptible: True, bootDiskSize: 20, image: \\"gcr.io/***REMOVED***-dev/***REMOVED***/wdl_runner:latest\\", logging: \\"gs://***REMOVED***-dev-from-personalis-gatk-logs/SHIP4946367/fastq-to-vcf/gatk-5-dollar/logs\\", diskSize: 1000, command: \\"java -Dconfig.file=${CFG} -Dbackend.providers.JES.config.project=${MYproject} -Dbackend.providers.JES.config.root=${ROOT} -jar /cromwell/cromwell.jar run ${WDL} --inputs ${INPUT} --options ${OPTION}\\", dryRun: True, labels: [\'Job\', \'Cromwell\'], input_CFG: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf\\", input_OPTION: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json\\", input_WDL: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl\\", input_SUBWDL: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl\\", input_INPUT: \\"gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json\\", env_MYproject: \\"***REMOVED***-dev\\", env_ROOT: \\"gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output\\", timeCreatedEpoch: 1559080699.59893, timeCreatedIso: \\"2019-05-28T21:58:19.598930+00:00\\"}) RETURN node", "result-mode": "data", "publish-topic": "wgs35-add-relationships", "result-structure": "list", "result-split": "False", "perpetuate": {"relationships": {"to-node": {"INPUT_TO": [{"basename": "SHIP4946367_2.ubam", "bucket": "***REMOVED***-dev-from-personalis-gatk", "contentType": "application/octet-stream", "crc32c": "ojStVg==", "dirname": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output", "etag": "CJTpxe3ynuICEAM=", "extension": "ubam", "generation": "1557970088457364", "id": "***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam/1557970088457364", "kind": "storage#object", "labels": ["WGS35", "Blob", "Ubam"], "md5Hash": "opGAi0f9olAu4DKzvYiayg==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam?generation=1557970088457364&alt=media", "metageneration": "3", "name": "SHIP4946367_2", "path": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam", "sample": "SHIP4946367", "selfLink": "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam", "size": 16886179620, "storageClass": "REGIONAL", "timeCreated": "2019-05-16T01:28:08.455Z", "timeCreatedEpoch": 1557970088.455, "timeCreatedIso": "2019-05-16T01:28:08.455000+00:00", "timeStorageClassUpdated": "2019-05-16T01:28:08.455Z", "timeUpdatedEpoch": 1558045261.522, "timeUpdatedIso": "2019-05-16T22:21:01.522000+00:00", "trellisTask": "fastq-to-ubam", "trellisWorkflow": "fastq-to-vcf", "updated": "2019-05-16T22:21:01.522Z"}, {"basename": "SHIP4946367_0.ubam", "bucket": "***REMOVED***-dev-from-personalis-gatk", "contentType": "application/octet-stream", "crc32c": "ZaJM+g==", "dirname": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output", "etag": "CM+sxKDynuICEAY=", "extension": "ubam", "generation": "1557969926952527", "id": "***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam/1557969926952527", "kind": "storage#object", "labels": ["WGS35", "Blob", "Ubam"], "md5Hash": "Tgh+eyIiKe8TRWV6vohGJQ==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam?generation=1557969926952527&alt=media", "metageneration": "6", "name": "SHIP4946367_0", "path": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam", "sample": "SHIP4946367", "selfLink": "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam", "size": 16871102587, "storageClass": "REGIONAL", "timeCreated": "2019-05-16T01:25:26.952Z", "timeCreatedEpoch": 1557969926.952, "timeCreatedIso": "2019-05-16T01:25:26.952000+00:00", "timeStorageClassUpdated": "2019-05-16T01:25:26.952Z", "timeUpdatedEpoch": 1558045265.901, "timeUpdatedIso": "2019-05-16T22:21:05.901000+00:00", "trellisTask": "fastq-to-ubam", "trellisWorkflow": "fastq-to-vcf", "updated": "2019-05-16T22:21:05.901Z"}]}}}}}'
    event = {'data': base64.b64encode(data)}
    result = query_db(event, context=None)

    # Create gatk-5-dollar relationship to ubam
    data = b'{"header": {"resource": "query", "method": "POST", "labels": ["Cypher", "Query", "Relationship", "Create"]}, "body": {"cypher": "\\n                            MATCH (related_node { basename: \\"SHIP4946367_0.ubam\\", bucket: \\"***REMOVED***-dev-from-personalis-gatk\\", contentType: \\"application/octet-stream\\", crc32c: \\"ZaJM+g==\\", dirname: \\"SHIP4946367/fastq-to-vcf/fastq-to-ubam/output\\", etag: \\"CM+sxKDynuICEAY=\\", extension: \\"ubam\\", generation: \\"1557969926952527\\", id: \\"***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam/1557969926952527\\", kind: \\"storage#object\\", labels: [\'WGS35\', \'Blob\', \'Ubam\'], md5Hash: \\"Tgh+eyIiKe8TRWV6vohGJQ==\\", mediaLink: \\"https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam?generation=1557969926952527&alt=media\\", metageneration: \\"6\\", name: \\"SHIP4946367_0\\", path: \\"SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam\\", sample: \\"SHIP4946367\\", selfLink: \\"https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam\\", size: 16871102587, storageClass: \\"REGIONAL\\", timeCreated: \\"2019-05-16T01:25:26.952Z\\", timeCreatedEpoch: 1557969926.952, timeCreatedIso: \\"2019-05-16T01:25:26.952000+00:00\\", timeStorageClassUpdated: \\"2019-05-16T01:25:26.952Z\\", timeUpdatedEpoch: 1558045265.901, timeUpdatedIso: \\"2019-05-16T22:21:05.901000+00:00\\", trellisTask: \\"fastq-to-ubam\\", trellisWorkflow: \\"fastq-to-vcf\\", updated: \\"2019-05-16T22:21:05.901Z\\" }), \\n                                  (node { image: \\"gcr.io/***REMOVED***-dev/***REMOVED***/wdl_runner:latest\\", dryRun: True, minCores: 1, input_SUBWDL: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl\\", input_WDL: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl\\", project: \\"***REMOVED***-dev\\", zones: \\"us-west1*\\", input_CFG: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf\\", command: \\"java -Dconfig.file=${CFG} -Dbackend.providers.JES.config.project=${MYproject} -Dbackend.providers.JES.config.root=${ROOT} -jar /cromwell/cromwell.jar run ${WDL} --inputs ${INPUT} --options ${OPTION}\\", labels: [\'Job\', \'Cromwell\'], diskSize: 1000, preemptible: True, provider: \\"google-v2\\", input_OPTION: \\"gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json\\", env_ROOT: \\"gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output\\", timeCreatedEpoch: 1559080699.59893, minRam: 6.5, logging: \\"gs://***REMOVED***-dev-from-personalis-gatk-logs/SHIP4946367/fastq-to-vcf/gatk-5-dollar/logs\\", timeCreatedIso: \\"2019-05-28T21:58:19.598930+00:00\\", env_MYproject: \\"***REMOVED***-dev\\", input_INPUT: \\"gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json\\", bootDiskSize: 20, user: \\"trellis\\" })\\n                            CREATE (related_node)-[:INPUT_TO]->(node)\\n                            ", "sent-from": "{DATA_GROUP}-add-relationships"}}'
    event = {'data': base64.b64encode(data)}
    result = query_db(event, context=None)
    sys.exit()

    try:
        # Create blob node
        data = {
                "resource": "query", 
                "neo4j-metadata": {
                                   "cypher": 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "***REMOVED***-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECEBE=", generation: "1555361455813565", id: "***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "17", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-04-23T19:17:53.205Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556047073.205, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-04-23T19:17:53.205000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, index: 0}) RETURN node',
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
                                   "cypher": 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "***REMOVED***-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECEBE=", generation: "1555361455813565", id: "***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "17", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-04-23T19:17:53.205Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556047073.205, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-04-23T19:17:53.205000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, index: 0}) RETURN node',
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
                                   "cypher": 'CREATE (node:Fastq:WGS_35000:Blob {bucket: "***REMOVED***-dev-from-personalis", componentCount: 32, contentType: "application/octet-stream", crc32c: "ftNG8w==", etag: "CL3nyPj80uECEBE=", generation: "1555361455813565", id: "***REMOVED***-dev-from-personalis/va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz/1555361455813565", kind: "storage#object", mediaLink: "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz?generation=1555361455813565&alt=media", metageneration: "17", name: "SHIP4946367_0_R1", selfLink: "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/va_mvp_phase2%2FDVALABP000398%2FSHIP4946367%2FFASTQ%2FSHIP4946367_0_R1.fastq.gz", size: 5955984357, storageClass: "REGIONAL", timeCreated: "2019-04-15T20:50:55.813Z", timeStorageClassUpdated: "2019-04-15T20:50:55.813Z", updated: "2019-04-23T19:17:53.205Z", path: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ/SHIP4946367_0_R1.fastq.gz", dirname: "va_mvp_phase2/DVALABP000398/SHIP4946367/FASTQ", basename: "SHIP4946367_0_R1.fastq.gz", extension: "fastq.gz", timeCreatedEpoch: 1555361455.813, timeUpdatedEpoch: 1556047073.205, timeCreatedIso: "2019-04-15T20:50:55.813000+00:00", timeUpdatedIso: "2019-04-23T19:17:53.205000+00:00", labels: [\'Fastq\', \'WGS_35000\', \'Blob\'], sample: "SHIP4946367", matePair: 1, index: 0}) RETURN node',
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

    # 
