import json

class PropertyTriggers:

    def __init__(self, project_id, properties):
        self.project_id = project_id

        self.added = {}
        self.removed = {}
        self.changed = {}
        self.nodes = {}

        for key, value in properties.items():
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
        for function, conditions in triggers.items():
            if set(conditions) == {True}:
                trigger_functions.append(function)
        self.unique_functions = set(trigger_functions)
        return self.unique_functions


    def fastq_to_ubam(self):
        topic = "wgs35-db-queries"
        topic_path = f"projects/{self.project_id}/topics/{topic}"

        sample = self.nodes['sample']

        message = {
                   "header": {
                              "resource": "query", 
                   }

                   "neo4j-metadata": {
                                        "cypher": (
                                                  "MATCH (n:Fastq) " + 
                                                  f"WHERE n.sample=\"{sample}\" " + 
                                                  "WITH n.readGroup AS read_group, " +
                                                  "n.setSize AS set_size, " +
                                                  "COLLECT(n) AS nodes " +
                                                  "WHERE size(nodes) = 2 " + 
                                                  "RETURN [n IN nodes] AS nodes, "
                                                  "set_size/2 AS metadata_setSize"), 
                                        "result-mode": "data",                
                   },
                   "trellis-metadata": {
                                        "publish-topic": "wgs35-tasks-fastq-to-ubam",
                                        "result-structure": "list",
                                        "result-split": "True"
                   }
        }
        return(topic_path, message)

