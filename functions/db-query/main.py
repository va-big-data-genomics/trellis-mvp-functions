import os
import json
import base64

from py2neo import Graph

from google.cloud import pubsub

# Environment variables
PROJECT_ID = os.environ.get('GOOGLE_CLOUD_PROJECT', '')
NEO4J_URL = os.environ.get('NEO4J_URL')
NEO4J_USER = os.environ.get('NEO4J_USER')
NEO4J_PASSPHRASE = os.environ.get('NEO4J_PASSPHRASE')

# Establish PubSub connection
PUBLISHER = pubsub.PublisherClient()

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
    data = json.loads(pubsub_message)
    print(data)

    # Check that resource is request
    if data['resource'] != 'query':
        print(f"Error: Expected resource type 'request', " +
              f"got '{data['resource']}.'")
        return
    
    trellis_metadata = data['trellis-metadata']
#    if trellis_metadata['function'] != 'wgs-9000-query-db':
#        print(f"Info: Skipping; request not for this function.")
#        return

    query = trellis_metadata['query']
    topic = trellis_metadata.get('publish_topic')

    print(f"Running db query: {query}.")
    if trellis_metadata['result'] == 'stats':
        result = GRAPH.run(query).stats()
        print(f"Result: {result}.")

        if topic:
            publish_to_topic(topic, result)
    
    elif trellis_metadata['result'] == 'nodes':
        nodes = GRAPH.run(query).data()
        print(f"Nodes count: {len(nodes)}.")
    
        if topic:
            print(f"Publishing nodes to {topic}.")
            for node in nodes:
                data = {
                        "resource": "node", 
                        "neo4j-metadata": node
                }
                publish_to_topic(topic, data)

if __name__ == "__main__": 
    PROJECT_ID = "***REMOVED***"
    NEO4J_URL = "https://35.247.30.111:7473"
    NEO4J_USER = "neo4j"
    NEO4J_PASSPHRASE = "QqkFQ8JHr3-IPReDqu3mXFpalWwpdrgz-UbpD7XifjiDL2XJtadb4zHoOjLta9mNfK2nttoSkbYkPrzbsLa9aA"

    GRAPH = Graph(
          NEO4J_URL, 
          user=NEO4J_USER, 
          password=NEO4J_PASSPHRASE)

    # Delete duplicate nodes
    """
    data = {
            "resource": "request", 
            "trellis-metadata": {
                                 "function": "wgs-9000-query-db", 
                                 "query": "MATCH (n) WITH n.id AS id, COLLECT(n) as nodes WHERE SIZE(nodes) > 1 UNWIND nodes[1..] AS n DETACH DELETE n", 
                                 "result": "stats"
            }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    query_db(event, context=None)
    """

    # Delete duplicate relationships
    """
    data ={
           "resource": "request", 
           "trellis-metadata": {
                                "function": "wgs-9000-query-db", 
                                "query": "MATCH (s)-[r]-(e) WITH s,e,type(r) as t, collect(r) as rels WHERE SIZE(rels) > 1 UNWIND rels[1..] AS r DELETE r", 
                                "result": "stats"
           }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    query_db(event, context=None)
    """

    # Get VCFs that need Vcfstats
    data = {
            "resource": "request", 
            "trellis-metadata": {
                                 "function": "wgs-9000-query-db", 
                                 "query": "MATCH (node:Vcf) WHERE node.bucket = '***REMOVED***-from-personalis' AND NOT (node)-[:RTG_TOOLS]->(:Vcfstats_data) RETURN node LIMIT 5", 
                                 "result": "nodes",
                                 "publish_topic": "wgs-9000-tasks-vcfstats"
            }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    query_db(event, context=None)

