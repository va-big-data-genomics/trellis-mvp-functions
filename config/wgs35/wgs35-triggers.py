import time

MAX_RETRIES = 3

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
                              "trigger": "AddFastqSetSize",
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
                              "trigger": "CheckUbamCount",
                              "publishTo": self.env_vars['TOPIC_GATK_5_DOLLAR'],
                   },
                   "body": {
                            "cypher": (
                                       "MATCH (n:Ubam) " +
                                       f"WHERE n.sample=\"{sample}\" " +
                                       "AND NOT (n)-[:INPUT_TO]->(:Job:Cromwell {name: \"gatk-5-dollar\"}) " +
                                       "WITH n.sample AS sample, " +
                                       "n.readGroup AS readGroup, " +
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
                              "trigger": "GetFastqForUbam",
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
                                       "RETURN [n IN nodes] AS nodes"
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
        reqd_header_labels = ['Update', 'Job', 'Node']

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
                              "labels": ["Duplicate", "Jobs", "Running", "Cypher", "Query", ],
                              "sentFrom": self.function_name,
                              "trigger": "KillDuplicateJobs",
                              "publishTo": [
                                            self.env_vars['TOPIC_KILL_JOB'], # Kill job
                                            self.function_name               # Label job as duplicate
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
                            "UNWIND tail(nodes) AS node " +
                            "RETURN node"
                        ),
                        "result-mode": "data",
                        "result-structure": "list",
                        "result-split": "True"
                   }
        }
        return([(topic, message)])


class MarkJobAsDuplicate:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # Only trigger when job node is created
        reqd_header_labels = ['Duplicate', 'Jobs', 'Database', 'Result']

        required_labels = ['Job']

        if not node:
            return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            set(required_labels).issubset(set(node.get('labels'))),
            not "Duplicate" in node.get('labels')
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node):
        """Mark duplicate job in the database.
        """
        topic = self.env_vars['DB_QUERY_TOPIC']

        instance_name = node['instanceName']

        query = self._create_query(instance_name)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "UPDATE",
                              "labels": ["Mark", "Duplicate", "Job", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "MarkJobAsDuplicate"
                   }, 
                   "body": {
                        "cypher": query,
                        "result-mode": "stats"
                   }
        }
        return([(topic, message)])


    def _create_query(self, instance_name):
        query = (
                  "MATCH (n:Job) " +
                 f"WHERE n.instanceName = \"{instance_name}\" " +
                  "SET n.labels = n.labels + \"Duplicate\", " +
                  "n:Marker, " +
                  "n.duplicate=True")
        return query


class RequeueJobQuery:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node=None):
        reqd_header_labels = ['Query', 'Cypher', 'Update', 'Job', 'Node']

        conditions = [
            header.get('method') == "UPDATE",
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
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
        header['trigger'] = "RequeueJobQuery"
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

        # Wait 2 seconds before re-queueing
        time.sleep(2)

        return([(topic, message)])


class RequeueRelationshipQuery:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node=None):
        reqd_header_labels = ['Relationship', 'Cypher', 'Query', ]

        conditions = [
            header.get('method') == "POST",
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            "Merge" in header.get('labels') or "Create" in header.get('labels'),
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
        header['trigger'] = "RequeueRelationshipQuery"
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

        # Wait 2 seconds before re-queueing
        time.sleep(2)

        return([(topic, message)])   


class RelateTrellisOutputToJob:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Blob', 'Node', 'Cypher', 'Query', 'Database', 'Result']

        if not node:
                return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            node.get("nodeIteration") == "initial",
            node.get("trellisTaskId"),
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

        node_id = node['id']
        task_id = node['trellisTaskId']

        query = self._create_query(node_id, task_id)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "Output", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateOutputToJob",
                              "publishTo": self.function_name
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)]) 

    def _create_query(self, node_id, task_id):
        query = (
                 f"MATCH (j:Job {{ trellisTaskId:\"{task_id}\" }} ), " +
                 f"(node:Blob {{trellisTaskId:\"{task_id}\", " +
                              f"id:\"{node_id}\" }}) " +
                  "WHERE NOT EXISTS(j.duplicate) " +
                  "OR NOT j.duplicate=True " +
                  "CREATE (j)-[:OUTPUT]->(node) " +
                  "RETURN node")
        return query


class RelateTrellisInputToJob:

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
            trellis_task_id = node['trellisTaskId']
            query = self._create_query(trellis_task_id, input_id)

            # Requeue original message, updating sentFrom property
            message = {
                       "header": {
                                  "resource": "query",
                                  "method": "POST",
                                  "labels": ["Create", "Relationship", "Input", "Cypher", "Query"],
                                  "sentFrom": self.function_name,
                                  "trigger": "RelatedInputToJob",
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

    def _create_query(self, trellis_task_id, input_id):
        query = (
                 f"MATCH (input:Blob {{ id:\"{input_id}\" }}), " +
                 f"(job:Job {{ trellisTaskId:\"{trellis_task_id}\"  }}) " +
                 f"CREATE (input)-[:INPUT_TO]->(job) " +
                  "RETURN job AS node")
        return query


class RunDstatWhenJobStopped:
    
    def __init__(self, function_name, env_vars):
        """Launch dstat after dsub jobs finish.
        """

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):
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

        messages = []
        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "command",
                              "method": "POST",
                              "labels": ["Dstat", "Command"],
                              "sentFrom": self.function_name,
                              "trigger": "RunDstatWhenJobStopped"
                   },
                   "body": {
                            "command": node["dstatCmd"]
                   }
        }
        return([(topic, message)])  


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
                              "labels": ["Create", "Dstat", "Relationship", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateDstatToJob",
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "stats",
                   }
        }
        return([(topic, message)])   


    def _create_query(self, node):
        query = (
                 "MATCH (job:Dsub " +
                    "{ " +
                        f"dsubJobId:\"{node['jobId']}\", " +
                        f"instanceName:\"{node['instanceName']}\" " +
                    "}), " +
                 "(dstat:Dstat " +
                    "{ " +
                        f"jobId:\"{node['jobId']}\", " +
                        f"instanceName:\"{node['instanceName']}\" " +
                    "}) " +
                  "WHERE NOT (job)-[:STATUS]->(dstat) " +
                  "CREATE (job)-[:STATUS]->(dstat) ")
        return query


class RecheckDstat:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Dstat', 'Node', 'Database', 'Result']

        if not node:
                return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            node.get("status") == "RUNNING",
            node.get("command")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True    


    def compose_message(self, header, body, node):
        topic = self.env_vars['TOPIC_DSTAT']

        message = {
                   "header": {
                              "resource": "command",
                              "method": "POST",
                              "labels": ["Dstat", "Command"],
                              "sentFrom": self.function_name,
                              "trigger": "RecheckDstat",
                   },
                   "body": {
                            "command": node["command"]
                   }
        }
        
        # Add retry count
        retry_count = header.get('retry-count')
        if retry_count:
            message["header"]["retry-count"] = retry_count + 1
        else:
            message["header"]["retry-count"] = 1
        
        # Wait 2 seconds before re-queueing
        time.sleep(2)

        return([(topic, message)])   


class RelateSampleToFromPersonalis:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']

        if not node:
                return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            "Sample" in node.get("labels"),
            node.get("sample"),
            node.get("bucket")
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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "Sample", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateSampleToFromPersonalis"
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "stats"
                   }
        }
        return([(topic, message)])  

    def _create_query(self, sample_node):
        sample = sample_node['sample']
        query = (
                 f"MATCH (j:Blob:Json:FromPersonalis:Sample {{ sample:\"{sample}\" }}), " +
                  "(b:Blob:FromPersonalis) " +
                  "WHERE b.sample = j.sample " +
                  "AND b.bucket = j.bucket " +
                  "AND NOT \"Sample\" IN labels(b) " +
                  "MERGE (j)-[:HAS]->(b)")
        return query


class RelateFromPersonalisToSample:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']

        if not node:
                return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            "FromPersonalis" in node.get("labels"),
            not "Sample" in node.get("labels"),
            node.get("sample"),
            node.get("bucket")
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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "Sample", "Blob", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateFromPersonalisToSample",
                              "publishTo": self.function_name   # Requeue message if fails initially
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",              # Allow message to be requeued
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])  

    def _create_query(self, node):
        sample = node['sample']
        bucket = node['bucket']
        path = node['path']
        query = (
                 f"MATCH (job:Blob:Json:FromPersonalis:Sample {{ sample:\"{sample}\" }}), " +
                 f"(node:Blob:FromPersonalis {{ bucket:\"{bucket}\", path:\"{path}\" }}) " +
                  "MERGE (job)-[:HAS]->(node) " +
                  "RETURN node")
        return query


# Track GATK workflow steps in database
"""
class MergeCromwellWorkflowStep:

    def __init__(self, function_name, env_vars):
        '''
            Triggered by: Create :CromwellAttempt
        '''

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Job', 'CromwellAttempt', 'Node', 'Database', 'Result']

        if not node:
            return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            node.get('cromwellWorkflowId'),  # Validate this is Cromwell step
            node.get('wdlCallAlias'),        # Get step name
            node.get('wdlTaskName'),
            node.get('status') == 'RUNNING'     # Only merge when attempt starts
            'CromwellAttempt' in node.get('labels')
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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "CromwellStep", "Node", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "MergeCromwellWorkflowStep",
                              "publishTo": self.function_name   # Requeue message if fails initially
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",              # Allow message to be requeued
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, node):
        cromwell_workflow_id = node['cromwellWorkflowId']
        wdl_call_alias = node['wdlCallAlias']
        wdl_task_name = node['wdlTaskName']
        query = (
                 "MERGE (node:CromwellStep {" +
                    f"cromwellWorkflowId: \"{cromwell_workflow_id}\" " +
                    f"wdlCallAlias: \"{wdl_call_alias}\" " +
                 "} " +
                 "ON CREATE SET " +
                    f"node.wdlTaskName= \"{wdl_task_name}\" " +
                     "node.nodeIteration=\"initial\" " +
                 "ON MERGE SET " +
                     "node.nodeIteration=\"merged\" " +
                 "RETURN node"
        )
        return query 
"""

class AddWorkflowIdToCromwellWorkflow:

    def __init__(self, function_name, env_vars):
        '''
            Triggered by: Blob created by GATK workflow.
        '''

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']

        if not node:
            return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            node.get('cromwellWorkflowId'),   # Need to add to Cromwell master
            node.get('trellisTaskId'),        # Use to match Cromwell master
            node.get('wdlCallAlias') == "ScatterIntervalList",
            node.get('basename') == "1scattered.interval_list",
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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "UPDATE",
                              "labels": ["Update", "CromwellWorkflow", "CromwellWorkflowId","Node", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "AddWorkflowIdToCromwellWorkflow",
                              "publishTo": self.function_name   # Requeue message if fails initially
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",              # Allow message to be requeued
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])  


    def _create_query(self, node):
        cromwell_workflow_id = node['cromwellWorkflowId']
        trellis_task_id = node['trellisTaskId']
        query = (
                 "MATCH (node:CromwellWorkflow) " +
                f"WHERE node.trellisTaskId = \"{trellis_task_id}\" " +
                f"SET node.cromwellWorkflowId = \"{cromwell_workflow_id}\" " +
                 "RETURN node"
        )
        return query 


"""
class RelateCromwellStepToWorkflow:


    def __init__(self, function_name, env_vars):
        '''Relate first Cromwell step to parent workflow.

        This trigger is activated only after Trellis has run a query 
        trying to relate the new step to the most recent step in the 
        workflow and gotten a null result. This indicates it is 
        the first step in the workflow and should be related
        '''

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # TODO: Change these
        reqd_header_labels = ['Create', 'CromwellStep', 'Node', 'Database', 'Result']

        if not node:
            return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Avoid infinite loop of queries from this trigger
            not 'CromwellWorkflow' in header.get('labels'),
            # Check node-specific information
            'CromwellStep' in node.get('labels'),
            node.get('cromwellWorkflowId'),
            node.get('nodeIteration') == "initial", # Only relate on creation
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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "CromwellStep", "CromwellWorkflow", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCromwellStepToWorkflow",
                              "publishTo": self.function_name   # Requeue message if fails initially
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "stats",
                   }
        }
        return([(topic, message)])


    def _create_query(self, node):
        cromwell_workflow_id = node['cromwellWorkflowId']
        wdl_call_alias = node['wdlCallAlias']
        query = (
                 f"MATCH " +
                    "(w:CromwellWorkflow { " +
                        f"cromwellWorkflowId: \"{cromwell_workflow_id}\" " +
                    "}), " +
                    "(s:CromwellStep { " +
                        f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                        f"wdlCallAlias: \"{wdl_call_alias}\" " +
                    "}) " +
                 "MERGE (w)-[:LED_TO]->(s)"
        )
        return query
"""

class RelateCromwellWorkflowToStep:
   

    def __init__(self, function_name, env_vars):
        '''Relate first Cromwell step to parent workflow.

        This trigger is activated only after Trellis has run a query 
        trying to relate the new step to the most recent step in the 
        workflow and gotten a null result. This indicates it is 
        the first step in the workflow and should be related
        '''

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ["Update", "CromwellWorkflow", "CromwellWorkflowId", "Node", "Database", "Result"]

        if not node:
            return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count')
                or header.get('retry-count') < MAX_RETRIES),
            # Only apply to :CromwellWorkflow nodes with ID
            'CromwellWorkflow' in node.get('labels'),
            node.get('cromwellWorkflowId'),
            # Check that workflow has not already been linked to steps
            (not node.get('cromwellStepConnected') 
                or node.get('cromwellStepConnected') != True)
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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "CromwellWorkflow", "CromwellStep", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCromwellWorkflowToStep",
                              "publishTo": self.function_name   # Requeue message if fails initially
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",              # Allow message to be requeued
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, node):
        cromwell_workflow_id = node['cromwellWorkflowId']
        query = (
                 f"MATCH " +
                    "(workflow:CromwellWorkflow { " +
                        f"cromwellWorkflowId: \"{cromwell_workflow_id}\" " +
                    "}), " +
                    "(step:CromwellStep { " +
                        f"cromwellWorkflowId: \"{cromwell_workflow_id}\" " +
                    "}) " +
                  "WITH workflow, COLLECT(step) AS steps, min(step.startTimeEpoch) AS minTime " +
                  "UNWIND steps AS step " +
                  "MATCH (step) " +
                  "WHERE step.startTimeEpoch = minTime " +
                  "MERGE (workflow)-[:LED_TO]->(step) " +
                  "RETURN workflow AS node"
        )
        return query


class RelateCromwellStepToPreviousStep:


    def __init__(self, function_name, env_vars):
        '''Relate new Cromwell step to most recent step in workflow.
        '''

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # TODO: Change these
        reqd_header_labels = ['Create', 'CromwellStep', 'Node', 'Database', 'Result']

        if not node:
            return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            'CromwellStep' in node.get('labels'),
            node.get('cromwellWorkflowId'),
            node.get('nodeIteration') == "initial", # Only relate on creation

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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "CromwellStep", "PreviousStep", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCromwellStepToPreviousStep",
                              "publishTo": self.function_name   # Requeue message if fails initially
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",              # Allow message to be requeued
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, node):
        cromwell_workflow_id = node['cromwellWorkflowId']
        wdl_call_alias = node['wdlCallAlias']

        query = (
                 "MATCH (previousStep:CromwellStep { " +
                            f"cromwellWorkflowId: \"{cromwell_workflow_id}\" " +
                        "}), " +
                        "(currentStep:CromwellStep { " +
                            f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                            f"wdlCallAlias: \"{wdl_call_alias}\" " +
                        "}) " +
                f"WHERE NOT previousStep.wdlCallAlias = \"{wdl_call_alias}\" " +
                 "AND previousStep.startTimeEpoch < currentStep.startTimeEpoch " +
                 "WITH currentStep, COLLECT(previousStep) AS steps, max(previousStep.startTimeEpoch) AS maxTime " +
                 "UNWIND steps AS step " +
                 "MATCH (step) " +
                 "WHERE step.startTimeEpoch = maxTime " +
                 "MERGE (step)-[:LED_TO]->(currentStep) " +
                 "RETURN currentStep AS node")
        return query


class CreateCromwellStepFromAttempt:


    def __init__(self, function_name, env_vars):
        '''Relate new Cromwell step to most recent step in workflow.
        '''

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # TODO: Change these
        reqd_header_labels = ['Create', 'Job', 'CromwellAttempt', 'Node', 'Database', 'Result']

        if not node:
            return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            'CromwellAttempt' in node.get('labels'),
            node.get('cromwellWorkflowId'),
            node.get('wdlCallAlias'),
            node.get('instanceName'),
            node.get('startTimeEpoch')
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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Node", "Relationship", "CromwellStep", "CromwellAttempt", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "CreateCromwellStepFromAttempt",
                              "publishTo": self.function_name   # Requeue message if fails initially
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",              # Allow message to be requeued
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, node):
        instance_name = node['instanceName']
        cromwell_workflow_id = node['cromwellWorkflowId']
        wdl_call_alias = node['wdlCallAlias']
        start_time_epoch = node['startTimeEpoch']
        query = (
                 "MATCH (attempt:Job { " +
                    f"instanceName: \"{instance_name}\" }}) " +
                 "MERGE (step:CromwellStep { " +
                    f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                    f"wdlCallAlias: \"{wdl_call_alias}\" " +
                  "}) " +
                 "ON CREATE SET " +
                    f"step.startTimeEpoch = {start_time_epoch}, " +
                     "step.labels = [\"CromwellStep\"], " +
                     "step.nodeIteration = \"initial\" " +
                 "ON MATCH SET " +
                     "step.nodeIteration = \"merged\" " +
                 "MERGE (step)-[:HAS_ATTEMPT]->(attempt) " +
                 "RETURN step AS node"
        )
        return query 


class RelateCromwellAttemptToPreviousAttempt:

    def __init__(self, function_name, env_vars):
        '''Relate new Cromwell attempt to last attempt in step.
        '''

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # TODO: Change these
        reqd_header_labels = ['Create', 'Job', 'CromwellAttempt', 'Node', 'Database', 'Result']

        if not node:
            return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            'CromwellAttempt' in node.get('labels'),
            node.get('cromwellWorkflowId'),
            node.get('wdlCallAlias'),
            node.get('instanceName')
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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "CromwellAttempt", "PreviousAttempt", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCromwellAttemptToPreviousAttempt",
                              "publishTo": self.function_name   # Requeue message if fails initially
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",              # Allow message to be requeued
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def __create_query(self, node):
        instance_name = node['instanceName']
        cromwell_workflow_id = node['cromwellWorkflowId']
        wdl_call_alias = node['wdlCallAlias']
        query = (
                 "MATCH (step:CromwellStep { " +
                        f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                        f"wdlCallAlias: \"{wdl_call_alias}\" " +
                    "})-[:HAS_ATTEMPT]->(oldAttempt:CromwellAttempt), " +
                    "(newAttempt:CromwellAttempt { " +
                        f"instanceName: \"{instance_name}\" " +
                    "}) " +
                f"WHERE oldAttempt.instanceName<>\"{instance_name}\" " +
                 "AND oldAttempt.startTimeEpoch < newAttempt.startTimeEpoch"
                 "MERGE (newAttempt)-[:AFTER]->(oldAttempt) " +
                 "RETURN oldAttempt AS node"
        )
        return query


    def _create_query(self, node):
        instance_name = node['instanceName']
        cromwell_workflow_id = node['cromwellWorkflowId']
        wdl_call_alias = node['wdlCallAlias']

        query = (
                 "MATCH (previousAttempt:CromwellAttempt { " +
                    f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                    f"wdlCallAlias: \"{wdl_call_alias}\" " +
                 "}), " +
                 "(currentAttempt:Job { " +
                    f"instanceName: \"{instance_name}\" " +
                 "}) " +
                f"WHERE NOT previousStep.instanceName = \"{instance_name}\" " +
                 "AND previousAttempt.startTimeEpoch < currentAttempt.startTimeEpoch " +
                 "WITH currentAttempt, COLLECT(previousAttempt) AS attempts, max(previousStep.startTimeEpoch) AS maxTime " +
                 "UNWIND attempts AS attempt " +
                 "MATCH (attempt) " +
                 "WHERE attempt.startTimeEpoch = maxTime " +
                 "MERGE (currentAttempt)-[:AFTER]->(previousAttempt) " +
                 "RETURN currentAttempt AS node")
        return query


class DeleteCromwellStepHasAttemptRelationship:
    

    def __init__(self, function_name, env_vars):
        '''Relate new Cromwell step to most recent step in workflow.
        '''

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # TODO: Change these
        reqd_header_labels = ['Create', 'Relationship', 'CromwellAttempt', 'PreviousAttempt', 'Database', 'Result']

        if not node:
            return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            'CromwellAttempt' in node.get('labels'),
            node.get('cromwellWorkflowId'),
            node.get('wdlCallAlias'),
            node.get('instanceName')
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

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Delete", "Relationship", "CromwellStep", "CromwellAttempt", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "DeleteCromwellStepHasAttemptRelationship",
                              "publishTo": self.function_name   # Requeue message if fails initially
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",              # Allow message to be requeued
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, node):
        #instance_name = node['instanceName']
        cromwell_workflow_id = node['cromwellWorkflowId']
        wdl_call_alias = node['wdlCallAlias']
        query = (
                  "MATCH (step:CromwellStep { " +
                    f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                    f"wdlCallAlias: \"{wdl_call_alias}\" " +
                  "})-[:HAS_ATTEMPT]->(newAttempt:CromwellAttempt)-[:AFTER]->(oldAttempt:CromwellAttempt) " +
                  "WITH step, newAttempt, oldAttempt " +
                  "MATCH (step)-[r:HAS_ATTEMPT]->(oldAttempt) " +
                  "DELETE r " +
                  "RETURN newAttempt AS node"
        )
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
    triggers.append(RelateTrellisOutputToJob(
                                    function_name,
                                    env_vars))
    triggers.append(RelateTrellisInputToJob(
                                    function_name,
                                    env_vars))
    triggers.append(RunDstatWhenJobStopped(
                                    function_name,
                                    env_vars))
    triggers.append(RelateDstatToJob(
                                    function_name,
                                    env_vars))
    triggers.append(RecheckDstat(
                                 function_name,
                                 env_vars))
    triggers.append(RelateFromPersonalisToSample(
                                    function_name,
                                    env_vars))
    triggers.append(MarkJobAsDuplicate(
                                    function_name,
                                    env_vars))

    ### Track GATK workflow steps
    #triggers.append(MergeCromwellWorkflowStep(
    #                                function_name,
    #                                env_vars))
    triggers.append(AddWorkflowIdToCromwellWorkflow(
                                    function_name,
                                    env_vars))
    #triggers.append(RelateCromwellStepToWorkflow(
    #                                function_name,
    #                                env_vars))
    triggers.append(RelateCromwellWorkflowToStep(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCromwellStepToPreviousStep(
                                    function_name,
                                    env_vars))
    triggers.append(CreateCromwellStepFromAttempt(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCromwellAttemptToPreviousAttempt(
                                    function_name,
                                    env_vars))
    #triggers.append(DeleteCromwellStepHasAttemptRelationship(
    #                                function_name,
    #                                env_vars))
    return triggers

