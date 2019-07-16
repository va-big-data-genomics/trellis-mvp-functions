
class AddFastqSetSize:

    def __init__(self, function_name, env_vars):
        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, node):

        required_labels = [
                           'Json',
                           'FromPersonalis',
                           'Marker']

        conditions = [
            set(required_labels).issubset(set(node.get('labels')))
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, node):
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample = node['sample']

        message = {
                   "header": {
                              "resource": "query",
                              "method": "UPDATE",
                              "labels": ["Cypher", "Query", "Set", "Properties"], 
                              "sentFrom": self.function_name,
                              "publishTo": self.env_vars['TRIGGER_TOPIC'],
                   },
                   "body": {
                          "cypher": (
                                     "MATCH (n:Fastq) " +
                                    f"WHERE n.sample=\"{sample}\" " +
                                     "WITH n.sample AS sample, " +
                                     "COLLECT(n) AS nodes " +
                                     "LIMIT 1 " +
                                     "UNWIND nodes AS node " +
                                     "SET node.setSize = size(nodes)" +
                                     "RETURN node " +
                                     "LIMIT 1"),
                          "result-mode": "data",
                          "result-structure": "list",
                          "result-split": "True",
                   }
        }
        return(topic, message)


class CheckUbamCount:

    def __init__(self, function_name, env_vars):
        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, node):

        required_labels = ['Ubam']

        conditions = [
            set(required_labels).issubset(set(node.get('labels')))
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, node):
        """Send full set of ubams to GATK task"""
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample = node['sample']
        set_size = node['setSize']

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Ubam", "GATK", "Nodes"],
                              "sentFrom": self.function_name,
                              "publishTo": self.env_vars['GATK_5_DOLLAR_TOPIC'],
                   },
                   "body": {
                            "cypher": (
                                       "MATCH (n:Ubam) " +
                                       f"WHERE n.sample=\"{sample}\" " +
                                       "AND NOT (n)-[:INPUT_TO]->(:Job:Cromwell {name: \"gatk-5-dollar\"}) " +
                                       "WITH n.sample AS sample, " +
                                       "n.setSize AS setSize, " +
                                       "COLLECT(n) as nodes " +
                                       "WHERE size(nodes) = setSize " +
                                       "RETURN nodes"),
                            "result-mode": "data", 
                            "result-structure": "list",
                            "result-split": "True",
                   }
        }
        return(topic, message)


class FastqToUbamTrigger:
    #TODO: Finish this class 

    def __init__(self, node):

        self.trigger = node
        self.check_conditions()

    def check_conditions(self):

        required_labels = [
                           'Blob', 
                           'Fastq', 
                           'WGS35', 
                           'FromPersonalis']

        #conditions:
        conditions = [
            self.node.get('setSize'),
            self.node.get('sample'),
            set(required_labels).issubset(set(node.get('labels')))
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


class FastqToUbamTrigger:

    def __init__(self):

    def check_conditions(self):

        required_labels = [
                           'Blob', 
                           'Fastq', 
                           'WGS35', 
                           'FromPersonalis']

        #conditions:
        self.node.get('setSize')
        self.node.get('sample')
        set(required_labels).issubset(s)

class PropertyTriggers:

    """OLD
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
    """

    def __init__(self, project_id, node):
        self.project_id = project_id
        self.node = node


    def get_triggers(self):
        #triggers = {
        #            self.fastq_to_ubam: [
        #                True if(self.added.get('setSize')) else False,
        #                True if(self.nodes.get('sample')) else False]
        #}

        triggers = {
                    self.fastq_to_ubam: [
                        True if (self.node.get('setSize')) else False,
                        True if (self.node.get('sample')) else False,
                        True if (set(['Blob','Fastq','WGS35','FromPersonalis']))]
        }


        trigger_functions = []
        for function, conditions in triggers.items():
            if set(conditions) == {True}:
                trigger_functions.append(function)
        self.unique_functions = set(trigger_functions)
        return self.unique_functions


    def fastq_to_ubam(self, function_name):
        topic = "wgs35-db-queries"
        #topic_path = f"projects/{self.project_id}/topics/{topic}"

        sample = self.nodes['sample']

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Nodes"],
                              "sentFrom": function_name,
                              "publishTo": "wgs35-tasks-fastq-to-ubam",
                   },
                   "body": {
                            "cypher": (
                                       "MATCH (n:Fastq) " + 
                                       f"WHERE n.sample=\"{sample}\" " + 
                                       "WITH n.readGroup AS read_group, " +
                                       "n.setSize AS set_size, " +
                                       "COLLECT(n) AS nodes " +
                                       "WHERE size(nodes) = 2 " + 
                                       "RETURN [n IN nodes] AS nodes, "
                                       "set_size/2 AS metadata_setSize"
                            ), 
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return(topic, message)