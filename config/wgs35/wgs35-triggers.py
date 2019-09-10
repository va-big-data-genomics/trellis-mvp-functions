class AddFastqSetSize:
    """Add setSize property to Fastqs and send them back to 
    triggers to launch fastq-to-ubam.
    """

    def __init__(self, function_name, env_vars):
        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        required_labels = [
                           'Json',
                           'FromPersonalis',
                           'Marker']

        if not node:
            return False

        conditions = [
            set(required_labels).issubset(set(node.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node):
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
        return([(topic, message)])


class CheckUbamCount:

    def __init__(self, function_name, env_vars):
        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # Only trigger GATK after relationship has been added
        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = ['Ubam']

        if not node:
            return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            set(required_labels).issubset(set(node.get('labels'))),
            node.get('setSize'),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node):
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
                                       "n.readGroup AS readGroup, " +
                                       "n.matePair AS matePair, " +
                                       "COLLECT(n) as allNodes " +
                                       "WITH head(allNodes) AS heads " +
                                       "UNWIND [heads] AS uniqueNodes " +
                                       "WITH uniqueNodes.sample AS sample, " +
                                       "uniqueNodes.setSize AS setSize, " +
                                       "COLLECT(uniqueNodes) AS sampleNodes " +
                                       "WHERE size(sampleNodes) = setSize " +
                                       "RETURN sampleNodes AS nodes"),
                            "result-mode": "data", 
                            "result-structure": "list",
                            "result-split": "True",
                   }
        }
        return([(topic, message)])


class GetFastqForUbam:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        required_labels = [
                           'Blob', 
                           'Fastq', 
                           'WGS35', 
                           'FromPersonalis']

        if not node:
            return False

        conditions = [
            node.get('setSize'),
            node.get('sample'),
            node.get('readGroup') == 0,
            node.get('matePair') == 1,
            set(required_labels).issubset(set(node.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node):
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
                                       "AND NOT (n)-[*2]->(:Ubam) " +
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
        return([(topic, message)])


class KillDuplicateJobs:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # Only trigger when job node is created
        reqd_header_labels = ['Create', 'Job', 'Node']

        required_labels = ['Job']

        if not node:
            return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            set(required_labels).issubset(set(node.get('labels'))),
            node.get('startTime'),
            node.get('instanceName'),
            node.get('instanceId'),
            node.get('inputHash'),
            node.get('status') == 'RUNNING',
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node):
        """
        Send results to 
            1) kill-duplicates to kill jobs and 
            2) triggers to mark job a duplicate in database
        """
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
                              # Why am I publishing this to kill-duplicates 
                              # and back to triggers?
                              "publishTo": [
                                            self.env_vars['TOPIC_KILL_DUPLICATES'],
                                            self.function_name
                              ]
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
        return([(topic, message)])


class RequeueJobQuery:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node=None):
        max_retries = 3
        reqd_header_labels = ['Query', 'Cypher', 'Update', 'Job', 'Node']

        conditions = [
            header.get('method') == "UPDATE",
            (not header.get('retry-count') 
             or header.get('retry-count') < max_retries),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            not node
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node):
        topic = self.env_vars['DB_QUERY_TOPIC']

        # Requeue original message, updating sentFrom property
        message = {}
        
        # Add retry count
        retry_count = header.get('retry-count')
        if retry_count:
            header['retry-count'] += 1
        else:
            header['retry-count'] = 1

        header['sentFrom'] = self.function_name
        header['resource'] = 'query'
        header['publishTo'] = self.function_name
        header['labels'].remove('Database')
        header['labels'].remove('Result')

        del(body['results'])
        body['result-mode'] = 'data'
        body['result-structure'] = 'list'
        body['result-split'] = 'True'

        message['header'] = header
        message['body'] = body

        return([(topic, message)])


class RequeueRelationshipQuery:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node=None):
        max_retries = 3
        reqd_header_labels = ['Relationship', 'Create', 'Cypher', 'Query', ]

        conditions = [
            header.get('method') == "POST",
            (not header.get('retry-count') 
             or header.get('retry-count') < max_retries),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            not node
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node):
        topic = self.env_vars['DB_QUERY_TOPIC']

        # Requeue original message, updating sentFrom property
        message = {}

        # Add retry count
        retry_count = header.get('retry-count')
        if retry_count:
            header['retry-count'] += 1
        else:
            header['retry-count'] = 1
        
        header['sentFrom'] = self.function_name
        header['resource'] = 'query'
        header['publishTo'] = self.function_name
        header['labels'].remove('Database')
        header['labels'].remove('Result')

        del(body['results'])
        body['result-mode'] = 'data'
        body['result-structure'] = 'list'
        body['result-split'] = 'True'

        message['header'] = header
        message['body'] = body

        return([(topic, message)])   


class RelateOutputToJob:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Node', 'Cypher', 'Query', 'Database', 'Result']

        if not node:
                return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            node.get("nodeIteration") == "initial",
            node.get("taskId"),
            node.get("id")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node):
        topic = self.env_vars['DB_QUERY_TOPIC']

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "Output", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "publishTo": self.function_name
                   },
                   "body": {
                            "cypher": (
                                       f"MATCH (j:Job {{ taskId:\"{node['taskId']}\" }} ), " +
                                       f"(node:Blob {{taskId:\"{node['taskId']}\", " +
                                                    f"id:\"{node['id']}\" }}) " +
                                       f"CREATE (j)-[:OUTPUT]->(node) " +
                                        "RETURN node"),
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])   


class RelatedInputToJob:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Job', 'Node', 'Database', 'Result']

        if not node:
                return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            "Job" in node.get("labels"),
            node.get("inputIds"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node):
        topic = self.env_vars['DB_QUERY_TOPIC']

        messages = []
        for input_id in node["inputIds"]:
            # Create a separate message for each related node
            query = self._create_query(node, input_id)

            # Requeue original message, updating sentFrom property
            message = {
                       "header": {
                                  "resource": "query",
                                  "method": "POST",
                                  "labels": ["Create", "Relationship", "Input", "Cypher", "Query"],
                                  "sentFrom": self.function_name,
                                  "publishTo": self.function_name
                       },
                       "body": {
                                "cypher": query,
                                "result-mode": "data",
                                "result-structure": "list",
                                "result-split": "True"
                       }
            }
            result = (topic, message)
            messages.append(result)
        return(messages)  

    def _create_query(self, job_node, input_id):
        query = (
                 f"MATCH (input:Blob {{ id:\"{input_id}\" }}), " +
                 f"(job:Job {{ taskId:\"{job_node['taskId']}\"  }}) " +
                 f"CREATE (input)-[:INPUT_TO]->(job) " +
                  "RETURN job AS node")
        return query


class RunDsubWhenJobStopped:
    
    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars



        reqd_header_labels = ['Update', 'Job', 'Node', 'Database', 'Result']

        if not node:
                return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            "Job" in node.get("labels"),
            node.get("status") == "STOPPED",
            node.get("dstatCmd")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node):
        topic = self.env_vars['TOPIC_DSTAT']
        publish_to = self.env_vars['DB_QUERY_TOPIC']

        messages = []
        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "command",
                              "method": "POST",
                              "labels": ["Dstat", "Command"],
                              "sentFrom": self.function_name,
                   },
                   "body": {
                            "command": node["dstatCmd"]
                   }
        }
        result = (topic, message)
        messages.append(result)
        return(messages)  


class RelateDstatToJob:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Dstat', 'Node', 'Database', 'Result']

        if not node:
                return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            node.get("jobId"),
            node.get("instanceName")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True    


    def compose_message(self, header, body, node):
        topic = self.env_vars['DB_QUERY_TOPIC']

        query = self._create_query(node)
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "Dstat", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "stats",
                   }
        }
        return([(topic, message)])   


    def _create_query(self, node):
        query = (
                 "MATCH (job:Dsub:Job " +
                    "{{ " +
                        f"dsubJobId:\"{node['jobId']}\", " +
                        f"instanceName:\"{node['instanceName']}\" " +
                    "}}), " +
                 "(dstat:Dstat " +
                    "{{ " +
                        f"jobId:\"{node['jobId']}\", " +
                        f"instanceName:\"{node['instanceName']}\" " +
                    "}})" +
                  "WHERE NOT (job)-[:STATUS]->(dstat) " +
                  "CREATE (job)-[:STATUS]->(dstat) ")
        return query


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
    triggers.append(RequeueJobQuery(
                                    function_name,
                                    env_vars))
    triggers.append(RequeueRelationshipQuery(
                                    function_name,
                                    env_vars))
    triggers.append(RelateOutputToJob(
                                    function_name,
                                    env_vars))
    triggers.append(RelatedInputToJob(
                                    function_name,
                                    env_vars))
    triggers.append(RunDsubWhenJobStopped(
                                    function_name,
                                    env_vars))
    triggers.append(RelateDstatToJob(
                                    function_name,
                                    env_vars))
    return triggers