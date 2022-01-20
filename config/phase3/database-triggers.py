import time

MAX_RETRIES = 3

class RequestUserPermissionsToDataset: 
    """ Connect a (:ServiceAccount) node to a (:Dataset) node.

    Connecting the service account to the dataset will grant the
    service account the level of permission described by the 
    relationships (e.g. "READ") to all objects in the dataset.

    Permissions are conferred by the manageAccountPermissions function
    which is controlled by this trigger.

    Supported roles:
        R: Read
        W: Write
        O: Owner

    Trigger pattern: https://lucid.app/lucidchart/8120f1c2-b3d0-40b5-b5a7-5d0490c195fc/edit?page=0_0#
    """

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Request', 'ServiceAccount', 'Permissions']

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            
            # Check that content of matches includes required fields/types
            body.get("ch-role") in ["R", "W", "O"],
            isInstance(body.get("user-email"), str),
            isInstance(body.get("dataset-name"), str),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id

        ch_role = body.get("ch-role")
        user_email = body.get("user-email")
        dataset_name = body.get("dataset")

        role_relationships = {
            "R": "HAS_READ_PERMISSIONS",
            "W": "HAS_WRITE_PERMISSIONS",
            "O": "HAS_OWNER_PERMISSIONS"
        }

        query = self._create_query(ch_role, user_email, dataset_name)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Cypher", "Query", "Request", "User", "Permissions", "Dataset"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestUserPermissionsToDataset",
                              "publishTo": self.env_vars['TOPIC_BLOB_UPDATE_USER_PERMISSIONS'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, ch_role, user_email, dataset_name):
        query = (
                 "MATCH (u:User), (d:Dataset) " +
                 f"WHERE u.email = {user_email} " +
                 f"AND d.name = {dataset_name} " +
                 "OPTIONAL MATCH (u)-[r:HAS_WRITE_PERMISSIONS|HAS_READ_PERMISSIONS|HAS_OWNER_PERMISSIONS]->(d) " +
                 "DELETE r " +
                 f"MERGE (u)-[r2:{rel_type} \{chRole: {ch_role}\}]->(d) " +
                 "WITH u, r, r2 d " +
                 "MATCH (d)-[:HAS]->(b:Blob) " +
                 "RETURN u.email AS userEmail, r.chRole AS revoke_chRole, r2.chRole AS grant_chRole, b AS blob"
                )
        return query


class RequestFastqToUbam:
    """ Initiate variant calling for all genomes.

    Initiate the first step in the variant calling workflow,
    FastqToUbam, for any genome in the database.

    Cypher query finds Fastqs ofgenomes that have not
    been processed and sends the Fastq node metadata back to 
    the check-triggers service so it will activate the
    "LaunchFastqToUbam" trigger.
    """

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Request', 'FastqToUbam', 'All']

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            body.get("limitCount"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id
        limit_count = body["limitCount"]

        query = self._create_query(event_id, limit_count)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Fastq", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestFastqToUbam",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id, limit_count):
        query = (
                 "MATCH (p:PersonalisSequencing)-[]->(f:Fastq) " +
                 "WHERE NOT (f)-[:WAS_USED_BY]->(:JobRequest:FastqToUbam) " +
                 f"WITH DISTINCT p LIMIT {limit_count} " +
                 "MATCH (p)-[:GENERATED]->(f:Fastq) " +
                 "WHERE f.matePair = 1 " +
                 "AND NOT (f)-[:WAS_USED_BY]->(:JobRequest:FastqToUbam) " +
                 "RETURN f AS node")
        return query


class RequestFastqToUbamCovid19:
    """ Initiate variant calling for Covid19 genomes.

    Initiate the first step in the variant calling workflow,
    FastqToUbam, for genomes of people included in the Covid19
    (:Study).

    Cypher query finds Fastqs of Covid19 genomes that have not
    been processed and sends the Fastq node metadata back to 
    the check-triggers service so it will activate the
    "LaunchFastqToUbam" trigger.
    """

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Request', 'FastqToUbam', 'Covid19']

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            body.get("limitCount"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id
        limit_count = body["limitCount"]

        query = self._create_query(event_id, limit_count)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Fastq", "Covid19", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestFastqToUbamCovid19",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id, limit_count):
        query = (
                 "MATCH (:Study {name:'Covid19'})-[*2]->(:Person)-[:GENERATED]->(:Sample)-[]->(p:PersonalisSequencing)-[]->(f:Fastq) " +
                 "WHERE NOT (f)-[:WAS_USED_BY]->(:JobRequest:FastqToUbam) " +
                 f"WITH DISTINCT p LIMIT {limit_count} " +
                 "MATCH (p)-[:GENERATED]->(f:Fastq) " +
                 "WHERE f.matePair = 1 " +
                 "AND NOT (f)-[:WAS_USED_BY]->(:JobRequest:FastqToUbam) " +
                 "RETURN f AS node")
        return query


class RequestLaunchGatk5Dollar:
    """Trigger for launching GATK $5 Cromwell workflow.

    Check whether all ubams for a sample are present, and
    that they haven't already been input to a $5 workflow.

    If so, send all ubam nodes metadata to the gatk-5-dollar
    pub/sub topic.
    """

    def __init__(self, function_name, env_vars):
        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # Only trigger GATK after relationship has been added
        reqd_header_labels = ['Request', 'LaunchGatk5Dollar', 'All']

        # If there are no results; trigger is not activated
        #if not node:
        #    return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            body.get("limitCount"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        #sample = node['sample']
        event_id = context.event_id
        seed_id = context.event_id
        limit_count = body["limitCount"]

        query = self._create_query(limit_count)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              # The 'Relationship' label is necessary to activate LaunchGatk5Dollar
                              "labels": ["Cypher", "Query", "Ubam", "Relationship", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestLaunchGatk5Dollar",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data", 
                            "result-structure": "list",
                            "result-split": "True",
                   }
        }
        return([(topic, message)])


    def _create_query(self, limit_count):
        # TODO: How do I implement the LIMIT logic here.
        ## I want to limit the number of genomes, not ubams
        ## SOLUTION: limit query to finding RG1
        query = (
                 "MATCH (s:PersonalisSequencing)" +
                 "-[:GENERATED]->(:Fastq)" +
                 "-[:WAS_USED_BY]->(:Job)" +
                 "-[:GENERATED]->(n:Ubam {readGroup:1}) " +
                 "WHERE NOT (s)-[*4]->(:JobRequest:Gatk5Dollar) " +
                 "RETURN DISTINCT n AS node " +
                 f"LIMIT {limit_count}")
        return query


class RequestLaunchFailedGatk5Dollar:
    """Trigger re-launching $5 GATK workflows that have failed.

    Check whether all ubams for a sample are present, and
    that they haven't already been input to a $5 workflow.
    If so, send all ubam nodes metadata to the gatk-5-dollar
    pub/sub topic.
    """

    def __init__(self, function_name, env_vars):
        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Request', 'LaunchFailedGatk5Dollar', 'All']

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        """Send full set of ubams to GATK task"""
        topic = self.env_vars['DB_QUERY_TOPIC']

        #sample = node['sample']
        event_id = context.event_id
        seed_id = context.event_id

        query = self._create_query(event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Ubam", "Failed", "GATK", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestLaunchFailedGatk5Dollar",
                              "publishTo": self.env_vars['TOPIC_GATK_5_DOLLAR'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data", 
                            "result-structure": "list",
                            "result-split": "True",
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id):
        """Check if all ubams for a sample are in the database & send to GATK $5 function.

        Update notes:
            v0.5.5: To reduce duplicate GATK $5 jobs caused by duplicate ubam objects,
                    check that sample is not related to an existing GATK $5 workflow. 
        """

        query = (
                 # Match GATK workflows that are stopped 
                 "MATCH (w:Gatk5Dollar:CromwellWorkflow) " +
                 # Group workflows by samples
                 "WITH w.sample AS sampleName, COLLECT(w) AS jobs, COLLECT(w.status) AS statuses " +
                 # Filter out any samples with running workflows
                 "WHERE NOT \"RUNNING\" in statuses " +
                 "UNWIND jobs AS w " +
                 "WITH sampleName, w " +
                 "MATCH (w)-[:STATUS]->(d:Dstat) " +
                 "WITH sampleName, COLLECT(d.status) AS statuses " +
                 # Select samples where none of the workflows have succeeded
                 "WHERE NOT \"SUCCESS\" IN statuses " +
                 "MATCH (s:PersonalisSequencing {sample:sampleName})" +  
                    "-[:GENERATED]->(:Fastq)" +                        
                    "-[:WAS_USED_BY]->(:Job)" +                      
                    "-[:GENERATED]->(n:Ubam), " +
                 "(s)-[:GENERATED]->(c:Checksum) " +
                 "WITH s.sample AS sample, " +
                   "c.fastqCount AS fastqSetSize " +                 
                   "n.readGroup AS readGroup, " +         
                   "COLLECT(DISTINCT n) AS allNodes " +
                 # Ignore duplicate nodes
                 "WITH fastqSetSize, head(allNodes) AS heads " +
                 "UNWIND [heads] AS uniqueNodes " +
                 "WITH uniqueNodes.sample AS sample, " +
                      "fastqSetSize, " +
                      "COLLECT(uniqueNodes) AS sampleNodes " +
                 "WHERE size(sampleNodes) = fastqSetSize/2 " +
                 # Create job request nodes
                 "CREATE (j:JobRequest:Gatk5Dollar {" +
                            "sample: sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: " +
                                "datetime().epochSeconds, " +
                            "name: \"gatk-5-dollar\", " +
                            f"eventId: {event_id} }}) " +
                 "WITH sampleNodes, " +
                      "sample, " +
                      "j.eventId AS eventId, " +
                      "j.nodeCreatedEpoch AS epochTime " +
                 "UNWIND sampleNodes AS sampleNode " +
                 # Merge ubam nodes to job request node
                 "MATCH (jobReq:JobRequest:Gatk5Dollar {" +
                            "sample: sample, " +
                            "eventId: eventId}) " +
                 "MERGE (sampleNode)-[:WAS_USED_BY]->(jobReq) " +
                 "RETURN DISTINCT(sampleNodes) AS nodes " +
                 "LIMIT 25")
        return query


class RequestGatk5DollarNoJob:
    """Trigger re-launching $5 GATK workflows that have failed.

    Check whether all ubams for a sample are present, and
    that they haven't already been input to a $5 workflow.
    If so, send all ubam nodes metadata to the gatk-5-dollar
    pub/sub topic.
    """

    def __init__(self, function_name, env_vars):
        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Request', 'LaunchFailedGatk5Dollar', 'All']

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        """Send full set of ubams to GATK task"""
        topic = self.env_vars['DB_QUERY_TOPIC']

        #sample = node['sample']
        event_id = context.event_id
        seed_id = context.event_id

        query = self._create_query(event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Ubam", "Failed", "GATK", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestGatk5DollarNoJob",
                              "publishTo": self.env_vars['TOPIC_GATK_5_DOLLAR'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data", 
                            "result-structure": "list",
                            "result-split": "True",
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id):
        """Check if all ubams for a sample are in the database & send to GATK $5 function.

        Description of query, by line:
            (1-2)   Find all ubams associated with this ubams sample.
            (3,8)   Check that there is not an existing GATK $5 workflow for this sample.
            (4-7)   Collect all ubams by sample/read group.
            (9-10)  In case there are duplicate ubams, only get the first node of each 
                    group of unique sample/read group ubams.
            (11-13) Group all the ubams with the same sample and setSize, where setSize
                    indicates how many ubams should be present for this sample.
            (14)    Check that the count of bams with unique read groups matches the 
                    expected number of bams.
            (15)    Return number of ubam nodes.

        Update notes:
            v0.5.5: To reduce duplicate GATK $5 jobs caused by duplicate ubam objects,
                    check that sample is not related to an existing GATK $5 workflow. 
        """
        query = (
                 f"MATCH (s:PersonalisSequencing)" +
                    "-[:GENERATED]->(:Fastq)" +
                    "-[:WAS_USED_BY]->(:Job)" +
                    "-[:GENERATED]->(n:Ubam)" +
                    "-[:WAS_USED_BY]->(jobRequest:JobRequest:Gatk5Dollar), " +
                 "(s)-[:GENERATED]->(check:Checksum) "
                 # Find samples with a $5 GATK job request & no job
                 "WHERE NOT (jobRequest)-[:TRIGGERED]->(:Job:Gatk5Dollar) "
                 # Don't launch job is another is currently running
                 "AND NOT (s)-[*4]->(:JobRequest:Gatk5Dollar)" + #5
                    "-[:TRIGGERED]->(:Job:Gatk5Dollar {status:\"RUNNING\"}) " +
                 # Don't launch job if another has succeeded
                 "AND NOT (s)-[*4]->(:JobRequest:Gatk5Dollar)" + #5
                    "-[:TRIGGERED]->(:Job:Gatk5Dollar {status:\"STOPPED\"})" +
                    "-[:STATUS]->(:Dstat {status:\"SUCCESS\"}) " +
                 # Create JobRequest node
                 "WITH s.sample AS sample, " +
                       "s.fastqCount AS fastqSetSize, " +
                       "n.readGroup AS readGroup, " +
                       "COLLECT(DISTINCT n) AS allNodes " +
                 "WITH fastqSetSize, head(allNodes) AS heads " +
                 "UNWIND [heads] AS uniqueNodes " +
                 "WITH uniqueNodes.sample AS sample, " +
                      "fastqSetSize, " +
                      "COLLECT(uniqueNodes) AS sampleNodes " +
                 "WHERE size(sampleNodes) = fastqSetSize/2 " +
                 "CREATE (j:JobRequest:Gatk5Dollar {" +
                            "sample: sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: " +
                                "datetime().epochSeconds, " +
                            "name: \"gatk-5-dollar\", " +
                            f"eventId: {event_id} }}) " +
                 # Send nodes to launch-gatk-5-dollar
                 "WITH sampleNodes, " +
                      "sample, " +
                      "j.eventId AS eventId, " +
                      "j.nodeCreatedEpoch AS epochTime " +
                 "UNWIND sampleNodes AS sampleNode " +
                 "MATCH (jobReq:JobRequest:Gatk5Dollar {" +
                            "sample: sample, " +
                            "eventId: eventId}) " +
                 "MERGE (sampleNode)-[:WAS_USED_BY]->(jobReq) " +
                 "RETURN DISTINCT(sampleNodes) AS nodes")
        return query


class LaunchGatk5Dollar:
    """Trigger for launching GATK $5 Cromwell workflow.

    It's activated by relationships, not nodes.

    Check whether all ubams for a sample are present, and
    that they haven't already been input to a $5 workflow.

    If so, send all ubam nodes metadata to the gatk-5-dollar
    pub/sub topic.
    """

    def __init__(self, function_name, env_vars):
        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # Only trigger GATK after relationship has been added
        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = ['Ubam']

        # If there are no results; trigger is not activated
        if not node:
            return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            set(required_labels).issubset(set(node.get('labels'))),
            #node.get('setSize'),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        """Send full set of ubams to GATK task"""
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample = node['sample']
        event_id = context.event_id

        query = self._create_query(sample, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Ubam", "GATK", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "LaunchGatk5Dollar",
                              "publishTo": self.env_vars['TOPIC_GATK_5_DOLLAR'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data", 
                            "result-structure": "list",
                            "result-split": "True",
                   }
        }
        return([(topic, message)])


    def _create_query(self, sample, event_id):
        """Check that all ubams are present for a sample and create a job request.

        Update notes:
            v0.5.5: To reduce duplicate GATK $5 jobs caused by duplicate ubam objects,
                    check that sample is not related to an existing GATK $5 workflow. 
        """
        query = (
                 # Find all ubams associated with this sample, and checksum object
                 f"MATCH (s:PersonalisSequencing {{sample:\"{sample}\"}})" +
                    "-[:GENERATED]->(:Fastq)" +
                    "-[:WAS_USED_BY]->(:Job)" +
                    "-[:GENERATED]->(n:Ubam), " +
                 "(s)-[:GENERATED]->(c:Checksum) " +
                 
                 # Don't start jobs if a job request already exists
                 "WHERE NOT (s)-[*4]->(:JobRequest:Gatk5Dollar) " +
                 
                 # Group ubams by read group
                 "WITH s.sample AS sample, " +
                    "c.fastqCount AS fastqSetSize, " +
                    "n.readGroup AS readGroup, " +
                    "COLLECT(DISTINCT n) AS allNodes " +
                 
                 # In case of duplicate ubams or nodes being generated
                 # for a read group, use the head() method to only get
                 # (1) ubam per read group
                 "WITH fastqSetSize, " +
                    "head(allNodes) AS heads " +
                 "UNWIND [heads] AS uniqueNodes " +
                 "WITH uniqueNodes.sample AS sample, " +
                      "fastqSetSize, " +
                      "COLLECT(uniqueNodes) AS sampleNodes " +
                 
                 # Check that the number of ubams matches the number of 
                 # fastqs divided by 2. Paired-end sequencing generates
                 # (2) fastqs per read group.
                 "WHERE size(sampleNodes) = fastqSetSize/2 " +
                 
                 # Create a job request, link the input nodes to the 
                 # request node, and return the input nodes so they 
                 # can be passed to the job launching function
                 "CREATE (j:JobRequest:Gatk5Dollar {" +
                            "sample: sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: " +
                                "datetime().epochSeconds, " +
                            "name: \"gatk-5-dollar\", " +
                            f"eventId: {event_id} }}) " +
                 "WITH sampleNodes, " +
                      "sample, " +
                      "j.eventId AS eventId, " +
                      "j.nodeCreatedEpoch AS epochTime " +
                 "UNWIND sampleNodes AS sampleNode " +
                 "MATCH (jobReq:JobRequest:Gatk5Dollar {" +
                            "sample: sample, " +
                            "eventId: eventId}) " +
                 "MERGE (sampleNode)-[:WAS_USED_BY]->(jobReq) " +
                 "RETURN DISTINCT(sampleNodes) AS nodes")
        return query


class LaunchFastqToUbam:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        required_labels = ['Blob', 'Fastq', 'WGS35', 'FromPersonalis']

        if not node:
            return False

        conditions = [
            node.get('sample'),
            isinstance(node.get('readGroup'), int),
            #node.get('matePair') == 1,
            set(required_labels).issubset(set(node.get('labels'))),
            
            # On/off switch to control whether variant calling
            #   should proceed in event-driven fashion.
            self.env_vars['WGS_VARIANT_CALLING'] == True,
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample = node['sample']
        read_group = node['readGroup']
        event_id = context.event_id

        query = self._create_query(sample, read_group, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Fastq", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "LaunchFastqToUbam",
                              "publishTo": self.env_vars['TOPIC_FASTQ_TO_UBAM'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, sample, read_group, event_id):
        query = (
                 # Find Fastqs from the same sample & read group
                 "MATCH (n:Fastq { " +
                            f"sample:\"{sample}\", " +
                            f"readGroup:{read_group} }}) " +
                 "WHERE NOT " +
                    "(n)-[:WAS_USED_BY]->(:JobRequest:FastqToUbam) " +
                 "WITH n.sample AS sample, " +
                      "n.matePair AS matePair, " +
                      "COLLECT(n) AS matePairNodes " +
                 # In the case of duplicate Fastqs, only use one from
                 #  each sequencing mate pair.
                 "WITH sample, " +
                      "COLLECT(head(matePairNodes)) AS uniqueMatePairs " +
                 # Check that there are a pair of Fastqs in the read group
                 #  (Paired-end sequencing)
                 "WHERE size(uniqueMatePairs) = 2 " + 
                 "CREATE (j:JobRequest:FastqToUbam { " +
                            "sample:sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"fastq-to-ubam\", " +
                            f"eventId: {event_id} }}) " +
                 "WITH uniqueMatePairs, " +
                     "j, " +
                     "sample, " +
                     "j.eventId AS eventId " +
                "UNWIND uniqueMatePairs AS uniqueMatePair " +
                "MERGE (uniqueMatePair)-[:WAS_USED_BY]->(j) " +
                "RETURN DISTINCT(uniqueMatePairs) AS nodes")
        return query


class RequestGetSignatureSnps:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Request', 'LaunchViewSignatureSnps', 'MergedVcf']

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            body.get("limitCount"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id
        limit_count = body["limitCount"]

        query = self._create_query(event_id, limit_count)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Vcf", "Tbi", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestGetSignatureSnps",
                              "publishTo": self.env_vars['TOPIC_VIEW_GVCF_SNPS'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id, limit_count):
        query = (
                 "MATCH (v:Merged:Vcf)-[:HAS_INDEX]->(t:Tbi) " +
                 "WHERE NOT " +
                    "(v)-[:WAS_USED_BY]->(:JobRequest:ViewGvcfSnps:SignatureSnps) " +
                 f"WITH v,t LIMIT {limit_count} " +
                 "CREATE (j:JobRequest:ViewGvcfSnps:SignatureSnps { " +
                            "sample:v.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"view-gvcf-snps\", " +
                            f"eventId: {event_id} }}) " +
                "MERGE (v)-[:WAS_USED_BY]->(j) " +
                "MERGE (t)-[:WAS_USED_BY]->(j) " +
                "RETURN v AS vcf, t AS index")
        return query


class RequestGetSignatureSnpsCovid19:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Request', 'LaunchViewSignatureSnps', 'Covid19']

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            body.get("limitCount"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id
        limit_count = body["limitCount"]

        query = self._create_query(event_id, limit_count)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "Vcf", "Tbi", "Covid19", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestGetSignatureSnpsCovid19",
                              "publishTo": self.env_vars['TOPIC_VIEW_GVCF_SNPS'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id, limit_count):
        query = (
                 "MATCH (:Person:Covid19)-[:HAS_BIOLOGICAL_OME]->(:Genome)-[:HAS_VARIANT_CALLS]->(v:Merged:Vcf)-[:HAS_INDEX]->(t:Tbi) " +
                 "WHERE NOT " +
                    "(v)-[:WAS_USED_BY]->(:JobRequest:ViewGvcfSnps:SignatureSnps) " +
                 f"WITH v,t LIMIT {limit_count} " +
                 "CREATE (j:JobRequest:ViewGvcfSnps:SignatureSnps { " +
                            "sample:v.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"view-gvcf-snps\", " +
                            f"eventId: {event_id} }}) " +
                "MERGE (v)-[:WAS_USED_BY]->(j) " +
                "MERGE (t)-[:WAS_USED_BY]->(j) " +
                "RETURN v AS vcf, t AS index")
        return query


class LaunchViewSignatureSnps:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        required_header_labels = ['Relate', 'Merged', 'Vcf', 'Tbi', 'Database', 'Result']
        required_labels = ['Blob', 'Merged', 'Vcf', 'WGS35']

        if not node:
            return False

        conditions = [
            # Only run on VCFs output from the 'MergeVCFs' step of the GATK pipeline
            node.get('wdlCallAlias') == 'MergeVCFs',
            node.get('sample'),
            node.get('id'),
            # Don't run on objects in pay-to-access storage classes (e.g. Nearline, Coldline)
            node.get('storageClass') == 'REGIONAL',
            set(required_labels).issubset(set(node.get('labels'))),
            # Only trigger once (:Vcf)-[:HAS_INDEX]->(:Tbi) relationship created
            set(required_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id
        node_id = node['id']

        query = self._create_query(event_id, seed_id, node_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Cypher", "Query", "SignatureSnps", "Vcf", "Tbi", "Nodes"],
                              "sentFrom": self.function_name,
                              "trigger": "LaunchViewSignatureSnps",
                              "publishTo": self.env_vars['TOPIC_VIEW_GVCF_SNPS'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id, seed_id, node_id):
        query = (
                 "MATCH (v:Merged:Vcf)-[:HAS_INDEX]->(t:Tbi) " +
                 f"WHERE v.id = \"{node_id}\" " +
                 "AND NOT (v)-[:WAS_USED_BY]->(:JobRequest:ViewGvcfSnps:SignatureSnps) " +
                 "CREATE (j:JobRequest:ViewGvcfSnps:SignatureSnps { " +
                            "sample:v.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"view-gvcf-snps\", " +
                            f"eventId: {event_id}, " +
                            f"seedId: {seed_id} }}) " +
                "MERGE (v)-[:WAS_USED_BY]->(j) " +
                "MERGE (t)-[:WAS_USED_BY]->(j) " +
                "RETURN v AS vcf, t AS index")
        return query


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
            node.get('status') == 'RUNNING', # Where does this status come from?
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
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
                                            self.env_vars['TOPIC_TRIGGERS'], # Label job as duplicate
                              ],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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


    def compose_message(self, header, body, node, context):
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
                              "trigger": "MarkJobAsDuplicate",
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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
                  "n:Duplicate, " +
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
            (not header.get('retry-count') or header.get('retry-count') < MAX_RETRIES),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            not node
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
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
        header['publishTo'] = self.env_vars['TOPIC_TRIGGERS']
        header['previousEventId'] = context.event_id
        
        header['labels'].remove('Database')
        header['labels'].remove('Result')

        del(body['results'])
        body['result-mode'] = 'data'
        body['result-structure'] = 'list'
        body['result-split'] = 'True'

        message['header'] = header
        message['body'] = body

        # Wait 2 seconds before re-queueing
        time.sleep(5)

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

    def compose_message(self, header, body, node, context):
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
        header['publishTo'] = self.env_vars['TOPIC_TRIGGERS']
        header['previousEventId'] = context.event_id

        header['labels'].remove('Database')
        header['labels'].remove('Result')

        del(body['results'])
        body['result-mode'] = 'data'
        body['result-structure'] = 'list'
        body['result-split'] = 'True'

        message['header'] = header
        message['body'] = body

        # Wait 2 seconds before re-queueing
        time.sleep(5)

        return([(topic, message)])   


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

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['TOPIC_DSTAT']

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "command",
                              "method": "POST",
                              "labels": ["Dstat", "Command"],
                              "sentFrom": self.function_name,
                              "trigger": "RunDstatWhenJobStopped",
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "command": node["dstatCmd"]
                   }
        }
        return([(topic, message)])  


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
            (not header.get('retry-count') or header.get('retry-count') < MAX_RETRIES),
            node.get("status") == "RUNNING",
            node.get("command")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True    


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['TOPIC_DSTAT']

        message = {
                   "header": {
                              "resource": "command",
                              "method": "POST",
                              "labels": ["Dstat", "Command"],
                              "sentFrom": self.function_name,
                              "trigger": "RecheckDstat",
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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
        time.sleep(5)

        return([(topic, message)])   


# Launch QC tasks
class LaunchBamFastqc:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = ['Blob', 'Bam', 'WGS35', 'Gatk']

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Metadata required for populating trigger query:
            node.get("id"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        event_id = context.event_id

        query = self._create_query(blob_id, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "FastQC", "Bam", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "LaunchBamFastqc",
                              "publishTo": self.env_vars['TOPIC_BAM_FASTQC'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, blob_id, event_id):
        query = (
                 f"MATCH (s:CromwellStep)-[:GENERATED]->(node:Blob:Bam) " +
                 "WHERE s.wdlCallAlias=\"gatherbamfiles\" " +
                 f"AND node.id =\"{blob_id}\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:JobRequest:BamFastqc) " +
                 "CREATE (jr:JobRequest:BamFastqc { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"bam-fastqc\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 1")
        return query


class LaunchFlagstat:
    
    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = ['Blob', 'Bam', 'WGS35', 'Gatk']

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Metadata required for populating trigger query:
            node.get("id"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        event_id = context.event_id

        query = self._create_query(blob_id, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "Flagstat", "Bam", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "LaunchFlagstat",
                              "publishTo": self.env_vars['TOPIC_FLAGSTAT'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, blob_id, event_id):
        query = (
                 f"MATCH (s:CromwellStep)-[:GENERATED]->(node:Blob:Bam) " +
                 "WHERE s.wdlCallAlias=\"gatherbamfiles\" " +
                 f"AND node.id =\"{blob_id}\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:JobRequest:Flagstat) " +
                 "CREATE (jr:JobRequest:Flagstat { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"flagstat\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 1")
        return query


class LaunchVcfstats:
    
    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Activate after creating (:Vcf)-[:INDEX]->(:Tbi) relationship
        reqd_header_labels = ['Database', 'Result', 'Relate', 'Tbi', 'Merged', 'Vcf']
        required_labels = [
                           'Blob', 
                           'Vcf',
                           'Merged',
                           'WGS35',
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Metadata required for populating trigger query:
            node.get("id"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        event_id = context.event_id

        query = self._create_query(blob_id, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "Launch", "Vcfstats", "Vcf", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "LaunchVcfstats",
                              "publishTo": self.env_vars['TOPIC_VCFSTATS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, blob_id, event_id):
        query = (
                 f"MATCH (s:CromwellStep)-[:GENERATED]->(node:Blob:Vcf) " +
                 "WHERE s.wdlCallAlias=\"mergevcfs\" " +
                 f"AND node.id =\"{blob_id}\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:JobRequest:Vcfstats) " +
                 "CREATE (jr:JobRequest:Vcfstats { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"vcfstats\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 1")
        return query


class LaunchTextToTable:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = [
                           'Blob', 
                           'Text',
                           'Data',
                           'WGS35',
        ]
        supported_labels = [
                            'Fastqc',
                            'Flagstat',
                            'Vcfstats'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            len(set(supported_labels).intersection(set(node.get('labels'))))==1,
            # Metadata required for populating trigger query:
            node.get("id"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        event_id = context.event_id

        query = self._create_query(blob_id, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "TextToTable", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "LaunchTextToTable",
                              "publishTo": self.env_vars['TOPIC_TEXT_TO_TABLE'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, blob_id, event_id):
        query = (
                 f"MATCH (:Job)-[:GENERATED]->(node:Blob) " +
                 f"WHERE node.id =\"{blob_id}\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:JobRequest:TextToTable) " +
                 "CREATE (jr:JobRequest:TextToTable { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"text-to-table\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 1")
        return query


class BigQueryImportCsv:
    
    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = [
                           'Blob',
                           'TextToTable',
                           'Data',
                           'WGS35',
        ]
        supported_labels = [
                            'Fastqc',
                            'Flagstat',
                            'Vcfstats'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            len(set(supported_labels).intersection(set(node.get('labels'))))==1,
            node.get('filetype') == 'csv',
            # Metadata required for populating trigger query:
            node.get("id"),
            node.get("sample")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        event_id = context.event_id

        query = self._create_query(blob_id, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "BigQueryImportCsv", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RunBigQueryImportCsv",
                              "publishTo": self.env_vars['TOPIC_BIGQUERY_IMPORT_CSV'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, blob_id, event_id):
        query = (
                 f"MATCH (:Job:TextToTable)-[:GENERATED]->(node:Blob) " +
                 f"WHERE node.id =\"{blob_id}\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:JobRequest:BigQueryImportCsv) " +
                 "CREATE (jr:JobRequest:BigQueryImportCsv { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"bigquery-import-csv\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 1")
        return query

""" Deprecated in favor of Postgres
class BigQueryImportContamination:
    
    
    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = [
                           'Blob',
                           'Data',
                           'Structured',
                           'Text',
                           'WGS35',
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            node.get("extension") == "preBqsr.selfSM",
            node.get("wdlCallAlias") == "CheckContamination",
            # Metadata required for populating trigger query:
            node.get("id"),
            node.get("sample")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        event_id = context.event_id

        query = self._create_query(blob_id, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "Import", "BigQuery", "Contamination", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "BigQueryImportContamination",
                              "publishTo": self.env_vars['TOPIC_BIGQUERY_APPEND_TSV'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, blob_id, event_id):
        query = (
                 f"MATCH (s:CromwellStep)-[:GENERATED]->(node:Blob) " +
                 f"WHERE node.id =\"{blob_id}\" " +
                 "AND s.wdlCallAlias = \"checkcontamination\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:JobRequest:BigQueryAppendTsv) " +
                 "CREATE (jr:JobRequest:BigQueryAppendTsv { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"bigquery-append-tsv\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 1")
        return query
"""

""" Deprecated in favor of Postgres
class RequestBigQueryImportContamination:


    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Request', 'BigQueryImportContamination']

        #if not node:
        #    return False

        conditions = [
            # Check that node matches metadata criteria:
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            #node.get("extension") == "preBqsr.selfSM",
            #node.get("wdlCallAlias") == "CheckContamination",
            # Metadata required for populating trigger query:
            #node.get("id"),
            #node.get("sample")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id

        query = self._create_query(event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "Import", "BigQuery", "Contamination", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestBigQueryImportContamination",
                              "publishTo": self.env_vars['TOPIC_BIGQUERY_APPEND_TSV'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id):
        query = (
                 f"MATCH (s:CromwellStep)-[:GENERATED]->(node:Blob) " +
                 f"WHERE node.extension =\"preBqsr.selfSM\" " +
                 "AND s.wdlCallAlias = \"checkcontamination\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:JobRequest:BigQueryAppendTsv) " +
                 "CREATE (jr:JobRequest:BigQueryAppendTsv { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"bigquery-append-tsv\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 1")
        return query
"""

class PostgresInsertCsv:
    
    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = [
                           'Blob',
                           'TextToTable',
                           'Data',
                           'WGS35',
        ]
        supported_labels = [
                            'Fastqc',
                            'Flagstat',
                            'Vcfstats'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            len(set(supported_labels).intersection(set(node.get('labels'))))==1,
            node.get('filetype') == 'csv',
            # Metadata required for populating trigger query:
            node.get("id"),
            node.get("sample")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        event_id = context.event_id

        query = self._create_query(blob_id, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "Insert", "Postgres", "Csv", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "PostgresInsertCsv",
                              "publishTo": self.env_vars['TOPIC_POSTGRES_INSERT_DATA'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, blob_id, event_id):
        query = (
                 f"MATCH (:Job:TextToTable)-[:GENERATED]->(node:Blob) " +
                 f"WHERE node.id =\"{blob_id}\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:JobRequest:PostgresInsertData) " +
                 "CREATE (jr:JobRequest:PostgresInsertData { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"postgres-insert-data\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 1")
        return query


class PostgresInsertContamination:
    
    
    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = [
                           'Blob',
                           'Data',
                           'Structured',
                           'Text',
                           'WGS35',
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            node.get("extension") == "preBqsr.selfSM",
            node.get("wdlCallAlias") == "CheckContamination",
            # Metadata required for populating trigger query:
            node.get("id"),
            node.get("sample")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        event_id = context.event_id

        query = self._create_query(blob_id, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "Insert", "Postgres", "Contamination", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "PostgresInsertContamination",
                              "publishTo": self.env_vars['TOPIC_POSTGRES_INSERT_DATA'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, blob_id, event_id):
        query = (
                 f"MATCH (s:CromwellStep)-[:GENERATED]->(node:Blob) " +
                 f"WHERE node.id =\"{blob_id}\" " +
                 "AND s.wdlCallAlias = \"checkcontamination\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:JobRequest:PostgresInsertData) " +
                 "CREATE (jr:JobRequest:PostgresInsertData { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"postgres-insert-data\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 1")
        return query


class RequestPostgresInsertContamination:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Request', 'PostgresInsertContamination']

        #if not node:
        #    return False

        conditions = [
            # Check that node matches metadata criteria:
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            #node.get("extension") == "preBqsr.selfSM",
            #node.get("wdlCallAlias") == "CheckContamination",
            # Metadata required for populating trigger query:
            #node.get("id"),
            #node.get("sample")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id

        query = self._create_query(event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "Import", "Postgres", "Contamination", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestPostgresInsertContamination",
                              "publishTo": self.env_vars['TOPIC_POSTGRES_INSERT_DATA'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id):
        query = (
                 f"MATCH (s:CromwellStep)-[:GENERATED]->(node:Blob) " +
                 f"WHERE node.extension =\"preBqsr.selfSM\" " +
                 "AND s.wdlCallAlias = \"checkcontamination\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:Job:PostgresInsertData) " +
                 "CREATE (jr:JobRequest:PostgresInsertData { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"postgres-insert-data\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 100")
        return query


class RequestPostgresInsertTextToTable:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        # Don't need to wait until
        reqd_header_labels = ['Request', 'PostgresInsertTextToTable']

        #if not node:
        #    return False

        conditions = [
            # Check that node matches metadata criteria:
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Metadata required for populating trigger query:
            #node.get("id"),
            #node.get("sample")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id

        query = self._create_query(event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "Import", "Postgres", "TextToTable", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestPostgresInsertTextToTable",
                              "publishTo": self.env_vars['TOPIC_POSTGRES_INSERT_DATA'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id):
        query = (
                 f"MATCH (node:Blob:TextToTable) " +
                 f"WHERE node.filetype =\"csv\" " +
                 "AND NOT (node)-[:WAS_USED_BY]->(:Job:PostgresInsertData) " +
                 "CREATE (jr:JobRequest:PostgresInsertData { " +
                            "sample: node.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"postgres-insert-data\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (node)-[:WAS_USED_BY]->(jr) " +
                 "RETURN node " +
                 "LIMIT 100")
        return query
# END Launch QC tasks


# Launch SV tasks
class RequestCnvnatorAll:
    """ Initiate variant calling for all genomes.

    Initiate the first step in the variant calling workflow,
    FastqToUbam, for any genome in the database.

    Cypher query finds Fastqs ofgenomes that have not
    been processed and sends the Fastq node metadata back to 
    the check-triggers service so it will activate the
    "LaunchFastqToUbam" trigger.
    """

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Request', 'Cnvnator', 'All']

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            body.get("limitCount"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id
        limit_count = body["limitCount"]

        query = self._create_query(event_id, limit_count)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              # ["Relate", "Cram", "Genome"] labels necessary to match LaunchCnvnator() header conditions
                              "labels": ["Trigger", "Request", "Cnvnator", "Relate", "Cram", "Genome", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestCnvnatorAll",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id, limit_count):
        query = (
                 "MATCH (:Person)-[:HAS_BIOLOGICAL_OME]->(:Genome)-[:HAS_SEQUENCING_READS]->(cram:Cram) " +
                 "WHERE NOT (cram)-[:WAS_USED_BY]->(:JobRequest:Cnvnator) " +
                 "RETURN DISTINCT cram AS node " +
                 f"LIMIT {limit_count}")
        return query


class RequestCnvnatorCovid19:
    """ Initiate variant calling for Covid19 genomes.

    Initiate the first step in the variant calling workflow,
    FastqToUbam, for genomes of people included in the Covid19
    (:Study).

    Cypher query finds Fastqs of Covid19 genomes that have not
    been processed and sends the Fastq node metadata back to 
    the check-triggers service so it will activate the
    "LaunchFastqToUbam" trigger.
    """

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Request', 'Cnvnator', 'Covid19']

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            body.get("limitCount"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id
        limit_count = body["limitCount"]

        query = self._create_query(event_id, limit_count)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              # ["Relate", "Cram", "Genome"] labels necessary to match LaunchCnvnator() header conditions
                              "labels": ["Trigger", "Request", "Cnvnator", "Covid19", "Relate", "Cram", "Genome", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestCnvnatorCovid19",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id, limit_count):
        query = (
                 "MATCH (:Study {name:'Covid19-Summer2021Pilot'})-[:HAS_PARTICIPANT|IS*2]->(:Person)-[:HAS_BIOLOGICAL_OME]->(:Genome)-[:HAS_SEQUENCING_READS]->(cram:Cram) " +
                 "WHERE NOT (cram)-[:WAS_USED_BY]->(:JobRequest:Cnvnator) " +
                 "RETURN DISTINCT cram AS node " +
                 f"LIMIT {limit_count}")
        return query


class LaunchCnvnator:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        # Need to wait until Cram has been related to the genome, because
        # query needs to get alignment coverage from (:PersonalisSequencing)
        reqd_header_labels = ['Relate', 'Cram', 'Genome', 'Database', 'Result']
        required_labels = [
                           'Cram',
                           'Gatk',
                           'Blob',
                           'WGS35'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Metadata required for populating trigger query:
            node.get("id"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        event_id = context.event_id

        query = self._create_query(blob_id, event_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "VIEW",
                              "labels": ["Trigger", "Launch", "Cnvnator", "Cram", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "LaunchCnvnator",
                              "publishTo": self.env_vars['TOPIC_CNVNATOR'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, blob_id, event_id):
        query = (
                 f"MATCH (cram:Blob:Cram)<-[:HAS_SEQUENCING_READS]-(:Genome)<-[:HAS_BIOLOGICAL_OME]-(person:Person)-[*2]->(p:PersonalisSequencing) " +
                 f"WHERE cram.id =\"{blob_id}\" " +
                 "AND NOT (cram)-[:WAS_USED_BY]->(:JobRequest:Cnvnator) " +
                 "OPTIONAL MATCH (person)<-[:IS]-(participant:Participant)<-[:HAS_PARTICIPANT]-(:Study {name:'Covid19-Summer2021Pilot'}) " +
                 "CREATE (jr:JobRequest:Cnvnator { " +
                            "sample: cram.sample, " +
                            "nodeCreated: datetime(), " +
                            "nodeCreatedEpoch: datetime().epochSeconds, " +
                            "name: \"cnvnator\", " +
                            f"eventId: {event_id} }}) " +
                 "MERGE (cram)-[:WAS_USED_BY]->(jr) " +
                 "RETURN cram, p.AlignmentCoverage AS alignmentCoverage, " +
                 "participant.study AS study, " +
                 "participant.hospitalized AS hospitalized, " +
                 "participant.recvdActureCare AS recvdActureCare, " +
                 "participant.stayedInIcu AS stayedInIcu " +
                 "LIMIT 1")
        return query


# Trellis v1.2 Data optimization triggers
class MergeBiologicalNodesFromSequencing:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']
        reqd_node_labels = [
                            'PersonalisSequencing',
                            'WGS35',
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(reqd_node_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),

            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            
            # Metadata required for populating trigger query:
            node.get("sample"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample = node['sample']

        query = self._create_query(sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Create", "Biological", "Nodes", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "MergeBiologicalNodesFromSequencing",
                              # Topic that db result of this trigger query will be published to
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, sample):
        query = (
                 "MATCH (s:PersonalisSequencing) " +
                 f"WHERE s.sample =\"{sample}\" " +
                 "MERGE (s)<-[:WAS_USED_BY {ontology: \"provenance\"}]-(:Sample:WgsPhase3 {sample: s.sample, labels: [\"Sample\", \"WgsPhase3\"]})<-[:GENERATED {ontology:\"provenance\"}]-(:Person {sample: s.sample, labels: [\"Person\"]})-[:HAS_BIOLOGICAL_OME {ontology:\"bioinformatics\"}]->(g:BiologicalOme:Genome {sample: s.sample, labels: [\"BiologicalOme\", \"Genome\"]}) " +
                 "RETURN g AS node")
        return query


class RelateGenomeToFastq:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Trigger', 'Create', 'Biological', 'Nodes', 'Database', 'Result']
        reqd_node_labels = [
                            'Genome',
                            'BiologicalOme',
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(reqd_node_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),

            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            
            # Metadata required for populating trigger query:
            node.get("sample"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample = node['sample']

        query = self._create_query(sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Genome", "Fastq", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateGenomeToFastq",
                              #"publishTo": self.env_vars['DB_QUERY_TOPIC'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, sample):
        query = (
                 "MATCH (g:Genome:BiologicalOme), (f:Blob:Fastq) " +
                 f"WHERE g.sample =\"{sample}\" " +
                 "AND f.sample = g.sample " +
                 "MERGE (g)-[:HAS_SEQUENCING_READS {ontology: \"bioinformatics\"}]->(f)")
        return query


## Data archival triggers
class ValidateGenomeRelationships:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        # Triggered by (:Merged:Vcf)-[:HAS_INDEX]->(:Tbi) relationship creation query
        required_header_labels = ['Database', 'Result', 'Relate', 'Genome', 'EssentialWgs']
        required_labels = ['Genome']

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(required_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample = node['sample']

        query = self._create_query(sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "UPDATE",
                              "labels": ["Trigger", "Validate", "Genome", "Relationships", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "ValidateGenomeRelationships",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, sample):
        query = (
                 "MATCH (s:Sample)<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(o:BiologicalOme:Genome) " +
                 f"WHERE s.sample =\"{sample}\" " +
                 "WITH s, o " +
                 "MATCH (o)-[:HAS_QC_DATA]->(:Fastqc), " +
                 "(o)-[:HAS_QC_DATA]->(:Flagstat), " +
                 "(o)-[:HAS_QC_DATA]->(:Vcfstats), " +
                 "(o)-[:HAS_SEQUENCING_READS]->(:Cram)-[:HAS_INDEX]->(:Crai), " +
                 "(o)-[:HAS_VARIANT_CALLS]->(:Merged:Vcf)-[:HAS_INDEX]->(:Tbi) " +
                 "SET s.trellis_optimizeStorage = true " +
                 "RETURN s AS node " +
                 "LIMIT 1")
        return query


# Triggered by results of ValidateGenomeRelationships
class DeleteNonessentialSequencingData:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        required_header_labels = ['Validate', 'Genome', 'Relationships', 'Database', 'Result']
        required_labels = ['Sample']

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(required_header_labels).issubset(set(header.get('labels'))),
            # Metadata required for populating trigger query:
            node.get("trellis_optimizeStorage") == True,
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample = node['sample']

        query = self._create_query(sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "UPDATE",
                              "labels": ["Trigger", "Validate", "Genome", "Relationships", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "ValidateGenomeRelationships",
                              "publishTo": self.env_vars['TOPIC_DELETE_BLOB'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "False"
                   }
        }
        return([(topic, message)])

    def _create_query(self, sample):
        query = (
                 "MATCH (s:PersonalisSequencing)-[:GENERATED|WAS_USED_BY|LED_TO*]->(b:Blob) " +
                 f"WHERE s.sample = \"{sample}\" " +
                 "WITH COLLECT(DISTINCT(b)) AS all_blobs " +
                 "UNWIND all_blobs AS b " +
                 "MATCH p=(b)-[*1..2]-(:BiologicalOme) " +
                 "WHERE ALL (r in relationships(p) WHERE r.ontology=\"bioinformatics\") " +
                 "WITH all_blobs, COLLECT(b) AS essential_blobs " +
                 "UNWIND all_blobs AS b " +
                 "MATCH (b) " +
                 "WHERE NOT b IN essential_blobs " +
                 "AND (NOT b.obj_exists = false OR NOT EXISTS(b.obj_exists)) " +
                 f"AND b.bucket = \"{self.env_vars['DSUB_OUT_BUCKET']}\" " + 
                 "RETURN b.bucket AS bucket, b.path AS path " +
                 "ORDER BY b.size DESC " +
                 "LIMIT 100")
        return query


# Triggered by results of ValidateGenomeRelationships
class MoveFastqsToColdline:
    """ Should be triggered by positive result of 
        ValidateGenomeRelationships trigger.
    """
    
    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        required_header_labels = ['Validate', 'Genome', 'Relationships', 'Database', 'Result']
        required_labels = ['Sample']

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(required_header_labels).issubset(set(header.get('labels'))),
            # Metadata required for populating trigger query:
            node.get("trellis_optimizeStorage") == True,
            # Only move Fastqs to coldline in production
            self.env_vars['GOOGLE_CLOUD_PROJECT'] == 'gbsc-gcp-project-mvp',
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        sample_id = node['sample']

        query = self._create_query(sample_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Fastq", "Coldline", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "MoveFastqsToColdline",
                              "publishTo": self.env_vars['TOPIC_BLOB_UPDATE_STORAGE'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, sample_id):
        query = (
                 "MATCH (s:Sample)-[:WAS_USED_BY]->(:PersonalisSequencing)-[:GENERATED]->(f:Fastq) " +
                 f"WHERE s.sample =\"{sample_id}\" " +
                 "AND s.trellis_optimizeStorage = True " +
                 "AND f.storageClass <> \"COLDLINE\" " +
                 "RETURN f AS node, \"COLDLINE\" AS requested_class")
        return query


class RequestChangeFastqStorage:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Request', 'Change', 'Fastq', 'Storage']

        request = body.get("request")
        if not request:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Metadata required for populating trigger query:
            request.get("count"),
            request.get("storage_class")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id

        request = body["request"]

        count = request["count"]
        storage_class = request["storage_class"]

        query = self._create_query(count, storage_class)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Fastq", "Coldline", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RequestMoveFastqsToColdline",
                              "publishTo": self.env_vars['TOPIC_BLOB_UPDATE_STORAGE'],
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, count, storage_class):
        query = (
                 "MATCH (s:Sample) " +
                 "WHERE s.trellis_snvQa=true " +
                 "AND NOT EXISTS(s.trellis_coldlineFastqs) " +
                 "WITH s " +
                 f"LIMIT {count} " +
                 "MATCH (s)-[:WAS_USED_BY]->(:PersonalisSequencing)-[:GENERATED]->(f:Fastq) " +
                 f"WHERE f.storageClass <> \"{storage_class}\" " +
                 "AND NOT f.storageClass IN [\"COLDLINE\", \"ARCHIVE\"] " +
                 "SET s.trellis_coldlineFastqs = localdatetime() " +
                 f"RETURN f AS node, \"{storage_class}\" AS requested_class")
        return query
## END Data archival triggers


class RelateVcfstatsToGenome:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']
        required_labels = [
                           'Vcfstats',
                           'Text',
                           'Data'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        sample = node['sample']

        query = self._create_query(blob_id, sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Vcfstats", "Genome", "EssentialWgs", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateVcfstatsToGenome",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id, sample):
        query = (
                 "MATCH (s:Sample)<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(ome:Genome:BiologicalOme), " +
                    "(blob:Blob:Vcfstats:Text:Data) " +
                 f"WHERE s.sample = \"{sample}\" " +
                 f"AND blob.id = \"{blob_id}\" " +
                 "MERGE (ome)-[:HAS_QC_DATA {ontology: \"bioinformatics\"}]->(blob) " +
                 "RETURN ome AS node")
        return query


class RelateFlagstatToGenome:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']
        required_labels = [
                           'Flagstat',
                           'Text',
                           'Data',
                           'WGS35',
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        sample = node['sample']

        query = self._create_query(blob_id, sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Flagstat", "Genome", "EssentialWgs", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateFlagstatToGenome",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id, sample):
        query = (
                 "MATCH (s:Sample)<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(ome:Genome:BiologicalOme), " +
                    "(blob:Blob:Flagstat:Text:Data:WGS35) " +
                 f"WHERE s.sample = \"{sample}\" " +
                 f"AND blob.id = \"{blob_id}\" " +
                 "MERGE (ome)-[:HAS_QC_DATA {ontology: \"bioinformatics\"}]->(blob) " +
                 "RETURN ome AS node")
        return query


class RelateFastqcToGenome:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']
        required_labels = [
                           'Fastqc',
                           'Text',
                           'Data',
                           'WGS35',
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        sample = node['sample']

        query = self._create_query(blob_id, sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Fastqc", "Genome", "EssentialWgs", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateFastqcToGenome",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id, sample):
        query = (
                 "MATCH (s:Sample)<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(ome:Genome:BiologicalOme), " +
                    "(blob:Blob:Fastqc:Text:Data:WGS35) " +
                 f"WHERE s.sample = \"{sample}\" " +
                 f"AND blob.id = \"{blob_id}\" " +
                 "MERGE (ome)-[:HAS_QC_DATA {ontology: \"bioinformatics\"}]->(blob) " +
                 "RETURN ome AS node")
        return query

# gVCF triggers
class RelateMergedVcfToGenome:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Database', 'Result', 'Relate', 'Merged', 'Vcf', 'Tbi']
        required_labels = [
                           'Vcf',
                           'Merged',
                           'Blob',
                           'WGS35'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        sample = node['sample']

        query = self._create_query(blob_id, sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Merged", "Vcf", "Genome", "EssentialWgs", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateMergedVcfToGenome",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id, sample):
        query = (
                 "MATCH (s:Sample)<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(ome:Genome:BiologicalOme), " +
                 "(blob:Blob:Merged:Vcf:WGS35) " +
                 f"WHERE s.sample = \"{sample}\" " +
                 f"AND blob.id = \"{blob_id}\" " +
                 "MERGE (ome)-[:HAS_VARIANT_CALLS {ontology: \"bioinformatics\"}]->(blob) " +
                 "RETURN ome AS node")
        return query


class RelateMergedVcfToTbi:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Relationship', 'Database', 'Result', 'Generated']
        required_labels = [
                           'Merged',
                           'Vcf',
                           'Gatk',
                           'Blob',
                           'WGS35'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']

        query = self._create_query(blob_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Merged", "Vcf", "Tbi", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateMergedVcfToTbi",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id):
        query = (
                 "MATCH (vcf:Blob:Merged:Vcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Blob:Tbi) " +
                 f"WHERE vcf.id =\"{blob_id}\" " +
                 "MERGE (vcf)-[:HAS_INDEX {ontology: \"bioinformatics\"}]->(tbi) " +
                 "RETURN vcf AS node")
        return query


class RelateTbiToMergedVcf:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Relationship', 'Database', 'Result', 'Generated']
        required_labels = ['Tbi', 'Gatk', 'Blob', 'WGS35']

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']

        query = self._create_query(blob_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Merged", "Vcf", "Tbi", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateTbiToMergedVcf",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id):
        query = (
                 "MATCH (vcf:Blob:Merged:Vcf)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(tbi:Blob:Tbi) " +
                 f"WHERE tbi.id =\"{blob_id}\" " +
                 "AND step.wdlCallAlias = \"mergevcfs\" " +
                 "MERGE (vcf)-[:HAS_INDEX {ontology: \"bioinformatics\"}]->(tbi) " +
                 "RETURN vcf AS node")
        return query
# END gVCF triggers


class RelateFastqToGenome:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']
        required_labels = [
                           'Fastq',
                           'FromPersonalis',
                           'Blob',
                           'WGS35'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        sample = node['sample']

        query = self._create_query(blob_id, sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Fastq", "Genome", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateFastqToGenome",
                              #"publishTo": self.env_vars['DB_QUERY_TOPIC'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id, sample):
        query = (
                 "MATCH (f:Blob:Fastq:FromPersonalis:WGS35), " +
                 "(g:BiologicalOme:Genome) " +
                 f"WHERE f.id = \"{blob_id}\" " +
                 "AND g.sample = f.sample " +
                 "MERGE (f)<-[:HAS_SEQUENCING_READS {ontology: \"bioinformatics\"}]-(g)")
        return query


# CRAM triggers
class RelateCramToGenome:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Relate', 'Cram', 'Crai', 'Database', 'Result']
        required_labels = [
                           'Cram',
                           'Gatk',
                           'Blob',
                           'WGS35'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']
        sample = node['sample']

        query = self._create_query(blob_id, sample)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Cram", "Genome", "EssentialWgs", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCramToGenome",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id, sample):
        query = (
                 "MATCH (s:Sample)<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(ome:Genome:BiologicalOme), " +
                 "(cram:Blob:Cram:Gatk:WGS35) " +
                 f"WHERE s.sample = \"{sample}\" " +
                 f"AND cram.id = \"{blob_id}\" " +
                 "MERGE (ome)-[:HAS_SEQUENCING_READS {ontology: \"bioinformatics\"}]->(cram) " +
                 "RETURN ome AS node")
        return query


class RelateCramToCrai:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = [
                           'Cram',
                           'Gatk',
                           'Blob',
                           'WGS35'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']

        query = self._create_query(blob_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Cram", "Crai", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCramToCrai",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id):
        query = (
                 "MATCH (cram:Blob:Cram)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(crai:Crai) " +
                 f"WHERE cram.id =\"{blob_id}\" " +
                 "MERGE (cram)-[:HAS_INDEX {ontology: \"bioinformatics\"}]->(crai) " +
                 "RETURN cram AS node")
        return query


class RelateCraiToCram:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        reqd_header_labels = ['Relationship', 'Database', 'Result']
        required_labels = [
                           'Crai',
                           'Gatk',
                           'Blob',
                           'WGS35'
        ]

        if not node:
            return False

        conditions = [
            # Check that node matches metadata criteria:
            set(required_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        blob_id = node['id']

        query = self._create_query(blob_id)

        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Trigger", "Relate", "Crai", "Cram", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCraiToCram",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)])

    def _create_query(self, blob_id):
        query = (
                 "MATCH (crai:Blob:Crai)<-[:GENERATED]-(step:CromwellStep)-[:GENERATED]->(cram:Cram) " +
                 f"WHERE crai.id =\"{blob_id}\" " +
                 "MERGE (cram)-[:HAS_INDEX {ontology: \"bioinformatics\"}]->(crai) " + 
                 "RETURN cram AS node")
        return query
# END CRAM triggers


# Relationship triggers
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
            node.get("id"),
            not node.get("wdlCallAlias")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        node_id = node['id']
        task_id = node['trellisTaskId']

        query = self._create_query(node_id, task_id)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "Trellis", "Generated", "Output", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateTrellisOutputToJob",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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
                 f"MERGE (j:Job {{trellisTaskId: \"{task_id}\" }}) " +
                 "ON CREATE SET j.labels = [\"Job\"] " +
                 "WITH j " +
                 "MATCH (node:Blob { " +
                    f"trellisTaskId: \"{task_id}\", " +
                    f"id: \"{node_id}\" " +
                 "}) " +
                 "MERGE (j)-[:GENERATED]->(node) " +
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

    def compose_message(self, header, body, node, context):
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
                                  "labels": ["Create", "Relationship", "Trellis", "Input", "Cypher", "Query"],
                                  "sentFrom": self.function_name,
                                  "trigger": "RelatedTrellisInputToJob",
                                  "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                                  "seedId": header["seedId"],
                                  "previousEventId": context.event_id,
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
                 f"CREATE (input)-[:WAS_USED_BY]->(job) " +
                  "RETURN job AS node")
        return query


class RelateJobToJobRequest:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):
        '''Input is job node after it has been related to inputs nodes.
        '''

        reqd_header_labels = ["Create", "Relationship", "Trellis", "Input"]

        if not node:
                return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            "Job" in node.get("labels")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        trellis_task_id = node["trellisTaskId"]

        query = self._create_query(trellis_task_id)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "Job", "JobRequest", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateJobToJobRequest",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)]) 

    def _create_query(self, trellis_task_id):
        '''
            Query objectives:
                * find job node for this job
                * find input blobs for this job
                * find job request(s) for those input blobs
                * check that job request is not related to job
                * Check that inputs to job are same as inputs to job request
        '''

        # NOTE: If this doesn't work, I can try also try using the
        # input IDs attached to the job node to find input blobs
        query = (
            f"MATCH (b:Blob)-[:WAS_USED_BY]->(j:Job {{ trellisTaskId: \"{trellis_task_id}\" }}), " +
            "(b)-[:WAS_USED_BY]->(jr:JobRequest {name: j.name}) " +
            "MATCH (b2:Blob)-[:WAS_USED_BY]->(jr) " +
            "WHERE NOT (jr)-[:TRIGGERED]->(:Job) " +
            "WITH j, jr, " +
            "COLLECT(DISTINCT b) AS jobInputs, " +
            "COLLECT(DISTINCT b2) AS requestInputs " +
            "WITH j, jr, jobInputs, requestInputs, " +
            "[b in jobInputs WHERE NOT b in requestInputs] AS mismatches, " +
            "[b in requestInputs WHERE NOT b in jobInputs] AS mismatches2 " +
            "WHERE size(mismatches) = size(mismatches2) = 0 " +
            "MERGE (jr)-[:TRIGGERED]->(j)")
        return query


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


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        query = self._create_query(node)
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Dstat", "Relationship", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateDstatToJob",
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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


class RelatePersonalisSequencingToFromPersonalis:

    def __init__(self, function_name, env_vars):
        '''NOTE: Currently not in use(?)

            I'm not sure why I created this. It seems like it's
            redundant with RelateFromPersonalisToSample.
        '''

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']
        reqd_node_labels = ['PersonalisSequencing']

        if not node:
                return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_node_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),

            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            node.get("sample"),
            node.get("bucket")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True    


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        query = self._create_query(node)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "Sample", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelatePersonalisSequencingToFromPersonalis",
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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
                 f"MATCH (s:Blob:Json:FromPersonalis:PersonalisSequencing {{ sample:\"{sample}\" }}), " +
                  "(b:Blob:FromPersonalis) " +
                  "WHERE b.sample = s.sample " +
                  "AND b.bucket = s.bucket " +
                  "AND NOT \"PersonalisSequencing\" IN labels(b) " +
                  "MERGE (s)-[:GENERATED]->(b)")
        return query


class RelateFromPersonalisToPersonalisSequencing:

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Blob', 'Node', 'Database', 'Result']
        reqd_node_labels = ['FromPersonalis']

        if not node:
                return False

        conditions = [
            # Check that message has appropriate headers
            set(reqd_node_labels).issubset(set(node.get('labels'))),
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            # Check that retry count has not been met/exceeded
            (not header.get('retry-count') 
             or header.get('retry-count') < MAX_RETRIES),
            # Check node-specific information
            not "PersonalisSequencing" in node.get("labels"),
            node.get("sample"),
            node.get("bucket")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True    


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        query = self._create_query(node)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "PersonalisSequencing", "Blob", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateFromPersonalisToPersonalisSequencing",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],   # Requeue message if fails initially
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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
                 f"MATCH (seq:Blob:Json:FromPersonalis:PersonalisSequencing {{ sample:\"{sample}\" }}), " +
                 f"(node:Blob:FromPersonalis {{ bucket:\"{bucket}\", path:\"{path}\" }}) " +
                  "MERGE (seq)-[:GENERATED]->(node) " +
                  "RETURN node")
        return query


# Track GATK workflow steps in database
class RelateCromwellOutputToStep:


    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):
        reqd_header_labels = ['Create', 'Blob', 'Node', 'Cypher', 'Query', 'Database', 'Result']

        if not node:
                return False

        conditions = [
            set(reqd_header_labels).issubset(set(header.get('labels'))),
            #node.get("nodeIteration") == "initial",
            node.get("trellisTaskId"),
            node.get("id"),
            node.get("wdlCallAlias")
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        query = self._create_query(node)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Generated", "Relationship", "CromwellStep", "Output", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCromwellOutputToStep",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            "result-split": "True"
                   }
        }
        return([(topic, message)]) 

    def _create_query(self, node):
        node_id = node['id']
        cromwell_workflow_id = node['cromwellWorkflowId']
        
        step_wdl_call_alias = node['wdlCallAlias'].lower()
        blob_wdl_call_alias = node['wdlCallAlias']
        query = (
                 "MATCH (step:CromwellStep { " +
                    f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                    f"wdlCallAlias: \"{step_wdl_call_alias}\" " +
                 "}), " +
                 "(node:Blob { " +
                    f"cromwellWorkflowId:\"{cromwell_workflow_id}\", " +
                    f"wdlCallAlias: \"{blob_wdl_call_alias}\", " +
                    f"id: \"{node_id}\" " +
                 "}) " +
                 "MERGE (step)-[:GENERATED]->(node) " +
                 "RETURN node")
        return query


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


    def compose_message(self, header, body, node, context):
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
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],   # Requeue message if fails initially
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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


    def compose_message(self, header, body, node, context):
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
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],   # Requeue message if fails initially
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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


    def compose_message(self, header, body, node, context):
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
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],   # Requeue message if fails initially
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        query = self._create_query(node)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Node", "CromwellStep", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "CreateCromwellStepFromAttempt",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],   # Requeue message if fails initially
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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
                 "RETURN step AS node"
        )
        return query 


class RelateCromwellStepToLatestAttempt:


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
            node.get('wdlCallAlias'),
            node.get('nodeIteration') == 'initial'
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        query = self._create_query(node)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "CromwellStep", "CromwellAttempt", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCromwellStepToAttempt",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],   # Requeue message if fails initially
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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
                 "MATCH (step:CromwellStep { " +
                            f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                            f"wdlCallAlias: \"{wdl_call_alias}\" " +
                        "}), " +
                        "(attempt:CromwellAttempt { " +
                            f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                            f"wdlCallAlias: \"{wdl_call_alias}\" " +
                        "}) " +
                 "WITH step, COLLECT(attempt) AS attempts, max(attempt.startTimeEpoch) AS maxTime " +
                 "UNWIND attempts AS attempt " +
                 "MATCH (attempt) " +
                 "WHERE attempt.startTimeEpoch = maxTime " +
                 "MERGE (step)-[:GENERATED_ATTEMPT]->(attempt) " +
                 "RETURN step AS node")
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
    

    def compose_message(self, header, body, node, context):
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
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],   # Requeue message if fails initially
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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

        query = (
                 "MATCH (previousAttempt:CromwellAttempt { " +
                    f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                    f"wdlCallAlias: \"{wdl_call_alias}\" " +
                 "}), " +
                 "(currentAttempt:Job { " +
                    f"instanceName: \"{instance_name}\" " +
                 "}) " +
                f"WHERE NOT previousAttempt.instanceName = \"{instance_name}\" " +
                 "AND previousAttempt.startTimeEpoch < currentAttempt.startTimeEpoch " +
                 "WITH currentAttempt, COLLECT(previousAttempt) AS attempts, max(previousAttempt.startTimeEpoch) AS maxTime " +
                 "UNWIND attempts AS attempt " +
                 "MATCH (attempt) " +
                 "WHERE attempt.startTimeEpoch = maxTime " +
                 "MERGE (currentAttempt)-[:AFTER]->(attempt) " +
                 "RETURN currentAttempt AS node")
        return query


class RelateCromwellStepToAttempt:
    

    def __init__(self, function_name, env_vars):
        '''When a new Cromwell attempt is added after a previous one, 
           create a new :GENERATED_ATTEMPT relationships between the step and 
           the newest attempt.
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
    

    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        query = self._create_query(node)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Create", "Relationship", "CromwellStep", "CromwellAttempt", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "RelateCromwellAttemptToPreviousAttempt",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],   # Requeue message if fails initially
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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

        query = (
                 "MATCH (step:CromwellStep { " +
                    f"cromwellWorkflowId: \"{cromwell_workflow_id}\", " +
                    f"wdlCallAlias: \"{wdl_call_alias}\" " +
                 "}), " +
                 "(attempt:Job { " +
                    f"instanceName: \"{instance_name}\" " +
                 "}) " +
                 "MERGE (step)-[:GENERATED_ATTEMPT]->(attempt) " +
                 "RETURN attempt AS node")
        return query


class DeleteRelationshipCromwellStepHasAttempt:
    

    def __init__(self, function_name, env_vars):
        '''Delete :GENERATED_ATTEMPT relationship between Cromwell step and old attempts
           once a newer attempt has been added to the database.
        '''

        self.function_name = function_name
        self.env_vars = env_vars


    def check_conditions(self, header, body, node):
        # TODO: Change these
        reqd_header_labels = ["Create", "Relationship", "CromwellStep", "CromwellAttempt", 'Database', 'Result']

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


    def compose_message(self, header, body, node, context):
        topic = self.env_vars['DB_QUERY_TOPIC']

        query = self._create_query(node)

        # Requeue original message, updating sentFrom property
        message = {
                   "header": {
                              "resource": "query",
                              "method": "POST",
                              "labels": ["Delete", "Relationship", "CromwellStep", "PreviousAttempt", "Cypher", "Query"],
                              "sentFrom": self.function_name,
                              "trigger": "DeleteRelationshipCromwellStepHasAttempt",
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],   # Requeue message if fails initially
                              "seedId": header["seedId"],
                              "previousEventId": context.event_id,
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
                  "})-[:GENERATED_ATTEMPT]->(newAttempt:CromwellAttempt)-[:AFTER*..5]->(oldAttempt:CromwellAttempt) " +
                  "WITH step, newAttempt, oldAttempt " +
                  "MATCH (step)-[r:GENERATED_ATTEMPT]->(oldAttempt) " +
                  "DELETE r " +
                  "RETURN newAttempt AS node"
        )
        return query 


def get_triggers(function_name, env_vars):

    triggers = []
    
    ### Launch variant-calling jobs
    triggers.append(LaunchGatk5Dollar(
                                    function_name,
                                    env_vars))
    triggers.append(LaunchFastqToUbam(
                                    function_name,
                                    env_vars))
    triggers.append(RequestFastqToUbam(
                                    function_name,
                                    env_vars))
    triggers.append(RequestFastqToUbamCovid19(
                                    function_name,
                                    env_vars))
    
    ## Request-driven triggers to re-launch failed/missing jobs
    triggers.append(RequestLaunchGatk5Dollar(
                                    function_name,
                                    env_vars))
    triggers.append(RequestLaunchFailedGatk5Dollar(
                                    function_name,
                                    env_vars))
    triggers.append(RequestGatk5DollarNoJob(
                                    function_name,
                                    env_vars))

    ## Launch QC jobs
    triggers.append(LaunchBamFastqc(
                                    function_name,
                                    env_vars))
    triggers.append(LaunchFlagstat(
                                    function_name,
                                    env_vars))
    triggers.append(LaunchVcfstats(
                                    function_name,
                                    env_vars))
    triggers.append(LaunchTextToTable(
                                    function_name,
                                    env_vars))
    """ Deprecated in favor of Postgres
    triggers.append(BigQueryImportCsv(
                                    function_name,
                                    env_vars))
    triggers.append(BigQueryImportContamination(
                                    function_name,
                                    env_vars))
    triggers.append(RequestBigQueryImportContamination(
                                    function_name,
                                    env_vars))
    """
    triggers.append(PostgresInsertCsv(
                                    function_name,
                                    env_vars))
    triggers.append(PostgresInsertContamination(
                                    function_name,
                                    env_vars))
    triggers.append(RequestPostgresInsertContamination(
                                    function_name,
                                    env_vars))
    triggers.append(RequestPostgresInsertTextToTable(
                                    function_name,
                                    env_vars))

    ### Launch SV/CNV jobs
    triggers.append(RequestCnvnatorAll(
                                    function_name,
                                    env_vars))
    triggers.append(RequestCnvnatorCovid19(
                                    function_name,
                                    env_vars))
    triggers.append(LaunchCnvnator(
                                    function_name,
                                    env_vars))

    ### Other
    triggers.append(KillDuplicateJobs(
                                    function_name,
                                    env_vars))
    triggers.append(RequeueJobQuery(
                                    function_name,
                                    env_vars))
    triggers.append(RequeueRelationshipQuery(
                                    function_name,
                                    env_vars))
    triggers.append(RunDstatWhenJobStopped(
                                    function_name,
                                    env_vars))

    triggers.append(RecheckDstat(
                                    function_name,
                                    env_vars))

    triggers.append(MarkJobAsDuplicate(
                                    function_name,
                                    env_vars))

    ### Trellis Relationship triggers
    triggers.append(RelateTrellisOutputToJob(
                                    function_name,
                                    env_vars))
    triggers.append(RelateTrellisInputToJob(
                                    function_name,
                                    env_vars))
    triggers.append(RelateJobToJobRequest(
                                    function_name,
                                    env_vars))
    triggers.append(RelateDstatToJob(
                                    function_name,
                                    env_vars))
    triggers.append(RelateFromPersonalisToPersonalisSequencing(
                                    function_name,
                                    env_vars))
    triggers.append(RelatePersonalisSequencingToFromPersonalis(
                                    function_name,
                                    env_vars))

    ### Track GATK workflow steps
    triggers.append(RelateCromwellOutputToStep(
                                    function_name,
                                    env_vars))
    triggers.append(AddWorkflowIdToCromwellWorkflow(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCromwellWorkflowToStep(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCromwellStepToPreviousStep(
                                    function_name,
                                    env_vars))
    triggers.append(CreateCromwellStepFromAttempt(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCromwellStepToLatestAttempt(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCromwellAttemptToPreviousAttempt(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCromwellStepToAttempt(
                                    function_name,
                                    env_vars))
    triggers.append(DeleteRelationshipCromwellStepHasAttempt(
                                    function_name,
                                    env_vars))

    ## Trellis v1.2 refactor
    triggers.append(MergeBiologicalNodesFromSequencing(
                                    function_name,
                                    env_vars))
    triggers.append(RelateGenomeToFastq(
                                    function_name,
                                    env_vars))
    triggers.append(ValidateGenomeRelationships(
                                    function_name,
                                    env_vars))
    triggers.append(DeleteNonessentialSequencingData(
                                    function_name,
                                    env_vars))
    triggers.append(RelateVcfstatsToGenome(
                                    function_name,
                                    env_vars))
    triggers.append(RelateFlagstatToGenome(
                                    function_name,
                                    env_vars))
    triggers.append(RelateFastqcToGenome(
                                    function_name,
                                    env_vars))
    triggers.append(RelateMergedVcfToGenome(
                                    function_name,
                                    env_vars))
    triggers.append(RelateFastqToGenome(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCramToGenome(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCramToCrai(
                                    function_name,
                                    env_vars))
    triggers.append(RelateCraiToCram(
                                    function_name,
                                    env_vars))
    triggers.append(RelateMergedVcfToTbi(
                                    function_name,
                                    env_vars))
    triggers.append(RelateTbiToMergedVcf(
                                    function_name,
                                    env_vars))
    triggers.append(RequestGetSignatureSnps(
                                    function_name,
                                    env_vars))
    triggers.append(RequestGetSignatureSnpsCovid19(
                                    function_name,
                                    env_vars))
    triggers.append(LaunchViewSignatureSnps(
                                    function_name,
                                    env_vars))
    triggers.append(MoveFastqsToColdline(
                                    function_name,
                                    env_vars))
    triggers.append(RequestChangeFastqStorage(
                                    function_name,
                                    env_vars))

    return triggers

