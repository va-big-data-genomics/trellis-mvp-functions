import json

from google.cloud import pubsub

PUBLISHER = pubsub.PublisherClient()

def publish_message(topic_path, message):
    message = json.dumps(message).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    print(f"Published message to {topic_path}: {result}.")   

class NodeTriggers:

    def __init__(self, project_id, node):
        self.project_id = project_id
        self.node = node

    def get_triggers(self):
        node_labels = self.node['node']['labels']

        triggers = {
                    'Json': self.trigger_fastq_to_ubam
        }

        trigger_functions = []
        for label in node_labels:
            trigger_function = triggers.get(label)
            if trigger_function:
                trigger_functions.append(trigger_function)
        # Get unique set of functions
        self.unique_functions = set(trigger_functions)
        return self.unique_functions

    def trigger_fastq_to_ubam(self):
        topic = "wgs-9000-db-queries"
        topic_path = f"projects/{self.project_id}/topics/{topic}"

        sample = self.node['node']['sample']

        message = {
                   "resource": "request", 
                   "trellis-metadata": {
                                        "query": (
                                                  "MATCH (n:Fastq) " + 
                                                  f"WHERE n.sample={sample} " + 
                                                  "WITH n.read_group AS read_group, " +
                                                  "collect(n) AS nodes " +
                                                  "WHERE size(nodes) = 2 " +
                                                  "RETURN [n in nodes] AS nodes"), 
                                        "result": "nodes", # lists?
                                        "publish_topic": "wgs-9000-tasks-fastq-to-ubam"
                   }
        }
        publish_message(topic_path, message)

    def trigger_csv_to_bigquery(self):
        topic = "wgs-9000-bigquery-csvs"
        topic_path = 'projects/{id}/topics/{topic}'.format(
                                                           id = self.project_id, 
                                                           topic = topic)
        message = {
                   "resource": "node", 
                   "neo4j-metadata": self.node
        }
        message = json.dumps(message).encode('utf-8')
        result = PUBLISHER.publish(topic_path, data=message).result()
        print(f"Published message to {topic_path}: {result}.")

    def execute_triggers(self):
        for trigger in self.unique_functions:
            # Execute trigger
            print(f"Executing trigger: {trigger}.")
            trigger()