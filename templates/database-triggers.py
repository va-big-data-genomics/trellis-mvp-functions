# Use a descriptive name for your trigger class.
class RequestCnvnator:
    """ Request Cnvnator dsub jobs for all genomes.
    """

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        # (Changes required) Use header labels to define 
        # contextual metadata that will activate the trigger
        required_header_labels = ['Request', 'FastqToUbam', 'Covid19']
        required_node_labels = []

        
        conditions = [
            set(required_header_labels).issubset(set(header.get('labels'))),
            set(required_node_labels).issubset(set(node.get('labels'))),
            
            # (Changes required) Add all your trigger conditions here. 
            # All statements must pass an "if" statement. The following 
            # values represent failure conditions: [0, None, False]
            body.get("limitCount"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    # (Changes required)
    def compose_message(self, header, body, node, context):
        
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id

        # (Changes required) Here is where you can define variables 
        # need to run your database query
        limit_count = body["limitCount"]

        # (Changes required) Always keep event_id, but provide
        # additional arguments as necessary to run your query
        query = self._create_query(event_id, limit_count)

        message = {
                   "header": {
                              "resource": "query",
                              # (Changes required): Define operation type in terms of CRUD [POST, GET, VIEW, etc.]
                              "method": "VIEW",
                              # (Changes required): Add metadata labels describing the query
                              "labels": ["Cypher", "Query", "Request", "Cnvnator", "Cram", "Relate", "Index"],
                              # (Do not change): Inidicates provenance of message
                              "sentFrom": self.function_name,
                              # (Changes required): Name of your trigger
                              "trigger": "RequestCnvnator",
                              # (Changes required): Determines where database result will be sent
                              "publishTo": self.env_vars['TOPIC_TRIGGERS'],
                              # (Do not change)
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            # (Changes optional): In the case the query returns multiple results,
                            # indicate whether each result should be sent in a separate message, 
                            # to a separate function instance (RECOMMENDED), or all results should
                            # be delivered in a single message.
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id, limit_count):
        # (Changes required) Replace with your database query
        query = (
                 "MATCH (cram:Cram)-[:INDEX]->(crai:Crai) " +
                 "WHERE NOT (cram)-[:WAS_USED_BY]->(:JobRequest:Cnvnator) " +
                 "RETURN cram AS node " +
                 f"LIMIT {limit_count}")
        return query


# Use a descriptive name for your trigger class.
class LaunchCnvnator:
    """ Launch a Cnvnator job the the received Cram.
    """

    def __init__(self, function_name, env_vars):

        self.function_name = function_name
        self.env_vars = env_vars

    def check_conditions(self, header, body, node):

        # (Changes required) Use header labels to define 
        # contextual metadata that will activate the trigger
        required_header_labels = ['Database', 'Result', 'Cram', "Relate", "Index"]
        required_node_labels = []

        
        conditions = [
            set(required_header_labels).issubset(set(header.get('labels'))),
            set(required_node_labels).issubset(set(node.get('labels'))),
            
            # (Changes required) Add all your trigger conditions here. 
            # All statements must pass an "if" statement. The following 
            # values represent failure conditions: [0, None, False]
            body.get("limitCount"),
        ]

        for condition in conditions:
            if condition:
                continue
            else:
                return False
        return True

    # (Changes required)
    def compose_message(self, header, body, node, context):
        
        topic = self.env_vars['DB_QUERY_TOPIC']

        event_id = context.event_id
        seed_id = context.event_id

        # (Changes required) Here is where you can define variables 
        # need to run your database query
        limit_count = body["limitCount"]

        # (Changes required) Always keep event_id, but provide
        # additional arguments as necessary to run your query
        query = self._create_query(event_id, limit_count)

        message = {
                   "header": {
                              "resource": "query",
                              # (Changes required): Define operation type in terms of CRUD [POST, GET, VIEW, etc.]
                              "method": "VIEW",
                              # (Changes required): Add metadata labels describing the query
                              "labels": ["Cypher", "Query", "Launch", "Cnvnator", "Cram", "Crai"],
                              # (Do not change): Inidicates provenance of message
                              "sentFrom": self.function_name,
                              # (Changes required): Name of your trigger
                              "trigger": "LaunchCnvnator",
                              # (Changes required): Determines where database result will be sent
                              "publishTo": self.env_vars['TOPIC_CNVNATOR'],
                              # (Do not change)
                              "seedId": seed_id,
                              "previousEventId": event_id,
                   },
                   "body": {
                            "cypher": query,
                            "result-mode": "data",
                            "result-structure": "list",
                            # (Changes optional): In the case the query returns multiple results,
                            # indicate whether each result should be sent in a separate message, 
                            # to a separate function instance (RECOMMENDED), or all results should
                            # be delivered in a single message.
                            "result-split": "True"
                   }
        }
        return([(topic, message)])


    def _create_query(self, event_id, cram_id):
        # (Changes required) Replace with your database query
        query = (
                 # Match the nodes that will be used as inputs
                 "MATCH (cram:Cram)-[:INDEX]->(crai:Crai) " +
                 f"WHERE cram.id = {cram_id} " +
                 # Check that a job request has not already been placed
                 "AND NOT (cram)-[:WAS_USED_BY]->(:JobRequest:Cnvnator) " +
                 # This job request node acts as a semaphore to any duplicate 
                 # queries that the job has already been requested so do not
                 # launch any more. This is necessary because there a significant
                 # delay between when a "Launch" trigger is activated and when
                 # the actual (:Job) node is created.
                 "CREATE (jr:JobRequest:Cnvnator { " +
                    "sample: node.sample, " +
                    "nodeCreated: datetime(), " +
                    "nodeCreatedEpoch: datetime().epochSeconds, " +
                    # Change this to match the lowercase name of your job
                    # (I forget why it's lowercase.)
                    "name: \"cnvnator\", " +
                    f"eventId: {event_id} }}) " +
                 "MERGE (cram)-[:WAS_USED_BY]->(jr) " +
                 "MERGE (crai)-[:WAS_USED_BY]->(jr) " +
                 "RETURN cram, crai " +
                 # Just in case there are duplicate nodes in the database.
                 # It shouldn't ever happen, but as a principle I generally
                 # build it multiple redundancies to catch anomalies.
                 "LIMIT 1")
        return query
