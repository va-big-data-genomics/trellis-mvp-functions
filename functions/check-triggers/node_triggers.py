import json

class NodeTriggers:

    def __init__(self, project_id, node):
        self.project_id = project_id
        self.node = node

    def get_triggers(self):
        node_labels = self.node['labels']

        #triggers = {
        #            'Json': self.add_fastq_set_size,
        #            'Ubam': self.check_ubam_count
        #}

        triggers = {
                    'Json,FromPersonalis,Marker': self.add_fastq_set_size,
                    'Ubam': self.check_ubam_count,
        }

        trigger_functions = []
        for label_set in triggers.keys():
            labels = set(label_set.split(','))
            if labels.issubset(set(node_labels)):
                trigger_function = triggers.get(label_set)
                trigger_functions.append(trigger_function)
        # Get unique set of functions
        self.unique_functions = set(trigger_functions)
        return self.unique_functions


        #trigger_functions = []
        #for label in node_labels:
        #    trigger_function = triggers.get(label)
        #    if trigger_function:
        #        trigger_functions.append(trigger_function)
        # Get unique set of functions
        #self.unique_functions = set(trigger_functions)
        #return self.unique_functions


    def add_fastq_set_size(self, function_name):
        topic = "wgs35-db-queries"

        sample = self.node['sample']

        message = {
                   "header": {
                              "resource": "query",
                              "method": "UPDATE",
                              "labels": ["Cypher", "Query", "Set", "Properties"], 
                              "sentFrom": function_name,
                              "publishTo": "wgs35-property-triggers",
                   },
                   "body": {
                          "cypher": (
                                     "MATCH (n:Fastq) " +
                                    f"WHERE n.sample=\"{sample}\" " +
                                     "WITH n.sample AS sample, " +
                                     "COLLECT(n) AS nodes " +
                                     "UNWIND nodes AS node " +
                                     "SET node.setSize = size(nodes)" +
                                     "RETURN DISTINCT " +
                                     "node.setSize AS `added_setSize`, " +
                                     "node.sample AS `nodes_sample`, " + 
                                     "node.labels AS `nodes_labels`"),
                          "result-mode": "data",
                          "result-structure": "list",
                          "result-split": "True",
                   }
        }
        return(topic, message)


    def check_ubam_count(self, function_name):
        """Send full set of ubams to GATK task"""
        topic = "wgs35-db-queries"

        sample = self.node['sample']
        set_size = self.node['setSize']

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Case", "Nodes"],
                              "sentFrom": function_name,
                              "publishTo": "wgs35-tasks-gatk-5-dollar",
                   },
                   "body": {
                            "cypher-old": (
                                     "MATCH (n:Ubam) " +
                                    f"WHERE n.sample=\"{sample}\" " +
                                     "WITH n.sample AS sample, " +
                                     "COLLECT(n) as nodes " +
                                     "RETURN " +
                                     "CASE " +
                                    f"WHEN size(nodes) = {set_size} " +
                                     "THEN nodes " +
                                     "ELSE NULL " +
                                     "END"),
                            "cypher": (
                                       "MATCH (n:Ubam) " +
                                       "WHERE n.sample=\"{sample}\" " +
                                       "WITH n.sample AS sample, " +
                                       "n.setSize AS setSize, " +
                                       "COLLECT(n) as nodes " +
                                       "WHERE size(nodes) = setSize " +
                                       "RETURN nodes"),
                            "result-mode": "data", 
                            "result-structure": "list",
                            "result-split": "False",
                   }
        }
        return(topic, message)

