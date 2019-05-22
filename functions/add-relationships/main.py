import os
import pdb
import sys
import json
import base64

from datetime import datetime

from google.cloud import pubsub

# Environment variables
ENVIRONMENT = os.environ.get('ENVIRONMENT')
if ENVIRONMENT == 'google-cloud':
    vars_blob = storage.Client() \
            .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
            .get_blob(os.environ['CREDENTIALS_BLOB']) \
            .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    TOPIC = parsed_vars['DB_QUERY_TOPIC']

    # Establish PubSub connection
    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{TOPIC}"


def publish_message(topic, query):
    message = {
               "resource": "query",
               "query": query,
               "trellis": {"sent-from": "add-relationships"},
    }
    publish_to_topic(topic, message)
    print(f"> Published following message to {topic}: {message}.")


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

    if data['resource'] != 'query-result':
        print(f"Error: Expected resource type 'blob', " +
              f"got '{data['resource']}.'")
        return

    node = data['results']
    relationships = data['relationships']
    
    # Get node labels
    labels = node['labels']
    print(f"Node labels: {labels}.\n")

    # Write a generic relationship query
    for orientation in relationships:
        for relationship_name in orientation:
            for related_node in relationship_name:
                
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
                            MATCH (related_node {related_string}), 
                                  (node {node_string})
                            CREATE (related_node)-[:{relationship_name}]->(node)
                            """
                elif orientation == "from-node":
                    query = f"""
                            MATCH (related_node {related_string}), 
                                  (node {node_string})
                            CREATE (node)-[:{relationship_name}]->(related_node)
                            """
                elif orientation == "bidirectional":
                    query = f"""
                            MATCH (related_node {related_string}), 
                                  (node {node_string})
                            CREATE (node)-[:{relationship_name}]-(related_node)
                            """
                # Compose query
                # CREATE (node)-[:{relationship_name}]->(related_node {related_node})
                publish_message(topic, query)


# For local testing
if __name__ == "__main__":
    PROJECT_ID = "***REMOVED***-dev"
    TOPIC_PATH = f"projects/{PROJECT_ID}/topics/{TOPIC}"

    data = {'resource': 'node', 'neo4j-metadata': {'node': {'extension': 'snvindel.var.vcf.gz', 'time-created-iso': '2019-03-11T23:08:55.327000+00:00', 'dirname': 'SHIP3935743/Variants', 'path': 'SHIP3935743/Variants/SHIP3935743.snvindel.var.vcf.gz', 'storageClass': 'REGIONAL', 'md5Hash': 'a7zCh6W1CLUca9JbkWBrmg==', 'timeCreated': '2019-03-11T23:08:55.327Z', 'time-updated-iso': '2019-03-26T23:23:02.512000+00:00', 'id': '***REMOVED***-dev-from-personalis/SHIP3935743/Variants/SHIP3935743.snvindel.var.vcf.gz/1552345735327945', 'contentType': 'text/vcard', 'generation': '1552345735327945', 'metageneration': '15', 'kind': 'storage#object', 'sample': 'SHIP3935743', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis/o/SHIP3935743%2FVariants%2FSHIP3935743.snvindel.var.vcf.gz?generation=1552345735327945&alt=media', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis/o/SHIP3935743%2FVariants%2FSHIP3935743.snvindel.var.vcf.gz', 'labels': ['Vcf', 'WGS_9000', 'Blob'], 'bucket': '***REMOVED***-dev-from-personalis', 'basename': 'SHIP3935743.snvindel.var.vcf.gz', 'crc32c': 'EMJeaA==', 'size': 224959007, 'timeStorageClassUpdated': '2019-03-11T23:08:55.327Z', 'name': 'SHIP3935743', 'etag': 'CMnh/cCa++ACEA8=', 'updated': '2019-03-26T23:23:02.512Z', 'time-created-epoch': 1552345735.327, 'time-updated-epoch': 1553642582.512}}}
    data = json.dumps(data)
    data = data.encode('utf-8')
    
    event = {'data': base64.b64encode(data)}

    add_relationships(event, context=None)
