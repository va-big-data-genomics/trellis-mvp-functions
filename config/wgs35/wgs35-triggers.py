class AddFastqSetSize:
    """Add setSize property to Fastqs and send them back to 
    triggers to launch fastq-to-ubam.
    """

    def __init__(self, function_name, env_vars):
        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, node):

        required_labels = [
                           'Json',
                           'FromPersonalis',
                           'Marker']

        conditions = [
            set(required_labels).issubset(set(node.get('labels'))),
            # (DISABLED) Only activate trigger on initial upload or
            #   metadata update.
            #(node['nodeIteration'] == 'initial' or 
            #    node['triggerOperation'] == 'metadataUpdate'),
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
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                   },
                   "body": {
                          "cypher": (
                                     "MATCH (n:Fastq) " +
                                    f"WHERE n.sample=\"{sample}\" " +
                                     "WITH n.sample AS sample, " +
                                     "COLLECT(n) AS nodes " +
                                     "UNWIND nodes AS node " +
                                     "SET node.setSize = size(nodes)" +
                                     "RETURN node "),
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
            node.get('setSize'),
            set(required_labels).issubset(set(node.get('labels'))),
            # Only activate trigger on initial upload or
            #   metadata update.
            #(node['nodeIteration'] == 'initial' or 
            #    node['triggerOperation'] == 'metadataUpdate'),
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
                              "publishTo": self.env_vars['TOPIC_GATK_5_DOLLAR'],
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


class GetFastqForUbam:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, node):

        required_labels = [
                           'Blob', 
                           'Fastq', 
                           'WGS35', 
                           'FromPersonalis']

        conditions = [
            node.get('setSize'),
            node.get('sample'),
            node.get('readGroup') == 0,
            node.get('matePair') == 1,
            set(required_labels).issubset(set(node.get('labels'))),
            # (DISABLED) Only activate trigger on initial upload or
            #   metadata update.
            #(node['nodeIteration'] == 'initial' or 
            #    node['triggerOperation'] == 'metadataUpdate'),
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
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Fastq", "Nodes"],
                              "sentFrom": self.function_name,
                              "publishTo": self.env_vars['TOPIC_FASTQ_TO_UBAM'],
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


class KillDuplicateJobs:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, node):

        required_labels = ['Job']

        conditions = [
            node.get('startTime'),
            node.get('instanceName'),
            node.get('instanceId'),
            node.get('inputHash'),
            node.get('status') == 'RUNNING']

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, node):
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample = node['sample']
        name = node['name']
        input_hash = node['inputHash']

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Duplicate", "Jobs", "Running"],
                              "sentFrom": self.function_name,
                              "publishTo": self.env_vars['TOPIC_KILL_DUPLICATES'],
                   }, 
                   "body": {
                        "cypher": (
                            "MATCH (n:Job) " +
                            f"WHERE n.sample = \"{sample}\" " +
                            f"AND n.name = \"{name}\" " +
                            f"AND n.inputHash = \"{input_hash}\" " +
                            "AND n.status = \"RUNNING\" " +
                            "WITH n.inputHash AS hash, " +
                            "COLLECT(n) AS nodes " +
                            "WHERE SIZE(nodes) > 1 " +
                            "RETURN tail(nodes) AS nodes"
                        ),
                        "result-mode": "data",
                        "result-structure": "list",
                        "result-split": "True"
                   }
        }
        return(topic, message)

def get_triggers(function_name, env_vars):

    triggers = []
    triggers.append(AddFastqSetSize(
                                    function_name,
                                    env_vars))
    triggers.append(CheckUbamCount(
                                   function_name,
                                   env_vars))
    triggers.append(GetFastqForUbam(
                                    function_name,
                                    env_vars))
    triggers.append(KillDuplicateJobs(
                                      function_name,
                                      env_vars))
    return triggers
