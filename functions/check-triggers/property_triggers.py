import json

from google.cloud import pubsub

PUBLISHER = pubsub.PublisherClient()

def publish_message(topic_path, message):
    message = json.dumps(message).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=message).result()
    print(f"Published message to {topic_path}: {result}.")   

class PropertyTriggers:

    def __init__(self, project_id, properties):
        self.project_id = project_id

        self.added = {}
        self.removed = {}
        self.changed = {}
        self.nodes = {}

        for key, value in properties.items()
            elements = key.split('_')
            if len(elements) > 2:
                printf("ERROR")
            category = elements[0]

            if category == 'added':
                self.added[elements[1]] = value
            elif category == 'removed':
                self.removed[elements[1]] = value
            elif category == 'changed':
                self.changed[elements[1]] = value
            elif category == 'nodes':
                self.nodes[elements[1]] = value
            else:
                printf("ERROR")


    def get_triggers(self):
        triggers = {
                   self.fastq_to_ubam: [
                        True if(self.added.get('setSize')) else False,
                        True if(self.nodes.get('sample')) else False]
        }

        trigger_functions = []
        for function, conditions in triggers:
            if set(conditions) == {True}:
                trigger_functions.append(function)
        self.unique_functions = set(trigger_functions)
        return self.unique_functions


    def fastq_to_ubam(self):
        topic = "wgs35-db-queries"
        topic_path = f"projects/{self.project_id}/topics/{topic}"

        sample = self.node['node']['sample']

        message = {
                   "resource": "query", 
                   "neo4j-metadata": {
                                        "cypher": (
                                                  "MATCH (n:Fastq) " + 
                                                  f"WHERE n.sample={sample} " + 
                                                  "WITH n.read_group AS read_group, " +
                                                  "collect(n) AS nodes " +
                                                  "WHERE size(nodes) = 2 " +
                                                  "RETURN [n in nodes] AS nodes"), 
                                        "result": "data",                
                   },
                   "trellis-metadata": {
                                        "publish_topic": "wgs35-tasks-fastq-to-ubam",
                                        "structure": "list",
                                        "split": "True"
                   }
        }
        publish_message(topic_path, message)

    def execute_triggers(self):
        for trigger in self.unique_functions:
            # Execute trigger
            print(f"Executing trigger: {trigger}.")
            trigger()