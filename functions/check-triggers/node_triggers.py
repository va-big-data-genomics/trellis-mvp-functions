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
        node_labels = self.node['labels']

        triggers = {
                    'Json': self.fastq_to_ubam,
                    'Ubam': self.check_ubam_count
        }

        trigger_functions = []
        for label in node_labels:
            trigger_function = triggers.get(label)
            if trigger_function:
                trigger_functions.append(trigger_function)
        # Get unique set of functions
        self.unique_functions = set(trigger_functions)
        return self.unique_functions

    def set_fastq_set_size(self):
        topic = "wgs35-db-queries"
        topic_path = f"project/{self.project_id}/topics/{topic}"

        sample = self.node['node']['sample']

        message = {
                   "resource": "query",
                   "neo4j-metadata": {
                                     "cypher": (
                                                "MATCH (n:Fastq) " +
                                               f"WHERE n.sample={sample} " +
                                                "WITH n.sample AS sample, " +
                                                "COLLECT(n) AS nodes " +
                                                "UNWIND nodes AS node " +
                                                "SET node.setSize = size(nodes)" +
                                                "RETURN DISTINCT " +
                                                "node.setSize AS `added_setSize`, " +
                                                "node.sample AS `nodes_sample`, " + 
                                                "node.labels AS `nodes_labels`")
                                    "result": "data"
                   },
                   "trellis-metadata": {
                                       "publish-topic": "wgs35-property-triggers",
                                       "result-structure": "list",
                                       "result-split": "True"
                   }
        }
        publish_message(topic_path, message)


    def check_ubam_count(self):
        """Send full set of ubams to GATK task"""
        topic = "wgs35-db-queries"
        topic_path = f"project/{self.project_id}/topics/{topic}"

        sample = self.node['node']['sample']
        set_size = self.node['node']['setSize']

        message = {
                   "resource": "query", 
                   "neo4j-metadata": {
                                      "cypher": (
                                                 "MATCH (n:Ubam) " +
                                                f"WHERE n.sample={sample} " +
                                                 "WITH n.sample AS sample, " +
                                                 "COLLECT(n) as ubams " +
                                                 "RETURN " +
                                                 "CASE " +
                                                 "WHEN size(ubams) = {set_size} " +
                                                 "THEN nodes " +
                                                 "ELSE NULL" +
                                                 "END"
                   }, 
                   "trellis-metadata": {
                                        "publish-topic": "wgs35-tasks-gatk-5-dollar", 
                                        "structure": "list",
                                        "split": "False"
                   }
        }
        publish_message(topic_path, message)

    def execute_triggers(self):
        for trigger in self.unique_functions:
            # Execute trigger
            print(f"Executing trigger: {trigger}.")
            trigger()