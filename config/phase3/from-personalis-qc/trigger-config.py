import json

from google.cloud import pubsub

PUBLISHER = pubsub.PublisherClient()

class NodeTriggers:

    def __init__(self, project_id, node):
        self.project_id = project_id
        self.node = node

    def get_triggers(self):
        node_labels = self.node['node']['labels']

        triggers = {
                    'Concat': self.trigger_csv_to_bigquery
        }

        trigger_functions = []
        for label in node_labels:
            trigger_function = triggers.get(label)
            if trigger_function:
                trigger_functions.append(trigger_function)
        # Get unique set of functions
        self.unique_functions = set(trigger_functions)
        return self.unique_functions

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
