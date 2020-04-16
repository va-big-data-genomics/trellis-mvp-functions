import json
import base64

from google.cloud import pubsub

PUBLISHER = pubsub.PublisherClient()

def format_pubsub_message(query):
    message = {
               "header": {
                          "resource": "query", 
                          "method": "POST",
                          "labels": ["Cypher", "Query", "Node", "Create"],
                          "sentFrom": f"publish-dummy-queries",
               },
               "body": {
                        "cypher": query,
                        "result-mode": "data",
                        "result-structure": "list",
                        "result-split": "True",
               },
    }
    return message

def format_output_node_query(db_dict, dry_run=False):
    """DEPRECATED since relationship logic moved to own function.
    """
    # Create label string
    labels_str = ':'.join(db_dict['labels'])

    # Create database entry string
    entry_strings = []
    for key, value in db_dict.items():
        if isinstance(value, str):
            entry_strings.append(f'{key}: "{value}"')
        else:
            entry_strings.append(f'{key}: {value}')
    entry_string = ', '.join(entry_strings)

    query = (
             f"MATCH (node:Test) WHERE EXISTS(node.count) " +
             f"MERGE (node)-[r:TEST]->(n:Test {{ {entry_string} }}) " +
             "RETURN node, r, n")

    # Format as cypher query
    #query = (
    #         f"MERGE (node:{labels_str} {{ {entry_string} }}) " +
    #          "RETURN node")
    return query

def publish_to_topic(topic, data):
    topic_path = PUBLISHER.topic_path(PROJECT_ID, topic)
    message = json.dumps(data).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    return result

def main(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(pubsub_message)
    header = data['header']
    body = data['body']

    publish_topic = header['publishTo']

    count = body['message-count']
    for i in range(count):
        db_dict = {
                   "labels": ["Test"],
                   "count": i
        }
        query = format_output_node_query(db_dict)
        message = format_pubsub_message(query)
        result = publish_to_topic(publish_topic, message)

if __name__ == "__main__":
    PROJECT_ID = "gbsc-gcp-project-mvp-dev"

    data = {
            "header": {
                       "publishTo": "dev-net-db-queries"
            },
            "body": {
                     "message-count": 100
            }
    }
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}
    main(event, None)
