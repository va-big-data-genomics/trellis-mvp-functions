import json
import neo4j
import base64

class DatabaseTrigger:
    """ A Trellis-formatted database query request.

    Use this class to generate query requests to be delivered to the Trellis
    'db-query' function.

    Attributes:
        name (str): Name of the trigger and corresponding database query. This name should
            be used to look up the query in the query config file.
        required_node_labels (list): These labels must be a subset of the node labels
            to activate trigger.
        recipient_topic (str): Name of the Trellis configuration key corresponding
            to the message broker topic the trigger message should be delivered to.
            In this most cases this will be the database function ('DB_QUERY_TOPIC').
        function_name (str): Name of the serverless function that is checking the 
            triggers.
        env_vars (dict): Trellis and computing environment configuration values.
        required_header_labels (list): Required labels from the incoming message header
            for activating the trigger.
        required_node_properties (dict): Node key:value properties that are required
            for activation. If the value is None only the presence of the property will
            be checked.
        required_body_properties (dict): Key:value properties that must be present in
            the incoming message body to activate trigger. If the value is None only 
            the presence of the property will be checked.
        required_node_property_types (dict): Specifies the type of value (str, int, etc.)
            that is required for node key:value properties.
        required_trellis_properties (dict): Key:value properties that must be present in
            the Trellis configuration provided in the env_vars.
        banned_node_labels (list): If a node has any of these labels it will not
            activate the trigger.
        results_mode (str): Determine whether to return query results or statistics. 
            Supported values : ["data", "stats"].
        node_required (bool): Indicates whether node metadata must be provided to activate
            the trigger. Node metadata is not required if trigger activation is being
            requested (e.g. Cloud Scheduler) rather than being event-driven.
        publish_results_to (str): Specify the message broker topic that the results of the 
            database query should be sent to.
        split_results (bool): Indicate whether to deliver all query results in a single message
            or split them into separate messages.
        write_transaction (bool): Determine whether to use the read or write Neo4j driver method.
        query_variables (dict): Dictionary where the key is the database trigger query
            variable name and the value is the key for the node property that will be passed
            as the variable value.
    """

    def __init__(
                 self, 
                 name,
                 required_node_labels,
                 recipient_topic,
                 function_name,
                 env_vars,
                 required_header_labels = [],
                 required_node_properties = {},
                 required_body_properties = {},
                 required_node_property_types = {},
                 required_trellis_properties = {},
                 banned_node_labels = [],
                 results_mode = "data",
                 node_required = True,
                 publish_results_to = None,
                 split_results = True,
                 write_transaction = False,
                 query_variables = {}):

        self.name = name
        self.required_node_labels = required_node_labels
        self.results_mode = results_mode
        self.recipient_topic = recipient_topic
        self.function_name = function_name
        self.env_vars = env_vars

        self.required_header_labels = required_header_labels
        self.required_node_properties = required_node_properties
        self.required_body_properties = required_body_properties
        self.required_node_property_types = required_node_property_types
        self.required_trellis_properties = required_trellis_properties
        self.banned_node_labels = banned_node_labels

        self.node_required = node_required
        self.publish_results_to = publish_results_to
        self.split_results = split_results
        self.write_transaction = write_transaction
        self.query_variables = query_variables

    def check_header_labels(self, header):
        # Check label conditions
        label_conditions = [
            set(self.required_header_labels).issubset(set(header.get('labels'))),
        ]

        for condition in label_conditions:
            if condition:
                continue
            else:
                return False
        return True

    def check_node_labels(self, node):
        # Check label conditions
        label_conditions = [
            # Check that node matches metadata criteria:
            set(self.required_node_labels).issubset(set(node.get('labels'))),
        ]

        for condition in label_conditions:
            if condition:
                continue
            else:
                return False
        return True

    def check_node_properties(self, node):
        # Check presence and and values of node properties
        for key, value in self.required_node_properties.items():
            if value:
                if node.get(key) == value:
                    continue
                else:
                    return False
            else:
                if node.get(key):
                    continue
                else:
                    return False
        return True

    def check_banned_node_labels(self, node):
        for banned_label in self.banned_node_labels:
            if banned_label in node['labels']:
                return False
        return True

    def check_node_property_instance_types(self, node):
        # Check node property instance types
        for property_name, property_type in self.required_node_property_types.items():
            if isinstance(node.get(property_name), property_type):
                continue
            else:
                return False
        return True

    def check_body_properties(self, body):
        # Check presence and and values of message body properties
        for key, value in self.required_body_properties.items():
            if value:
                if body.get(key) == value:
                    continue
                else:
                    return False
            else:
                if body.get(key):
                    continue
                else:
                    return False
        return True

    def check_trellis_configuration(self, env_vars):
        for key, value in self.required_trellis_properties.items():
            if env_vars.get(key) == value:
                continue
            else:
                return False
        return True

    def check_all_conditions(self, header, body, node):

        results = []
        results.append(self.check_header_labels(header))
        results.append(self.check_node_labels(node))
        results.append(self.check_node_properties(node))
        results.append(self.check_node_property_instance_types(node))
        results.append(self.check_body_properties(body))
        results.append(self.check_banned_node_labels(node))
        if False in results:
            return False
        else:
            return True

    def format_json_messages(self, header, body, node, context):

        query_parameters = _get_query_parameters()

        # Instead of directly creating the message, I should provide
        # the inputs to the TrellisMessage class and create it 
        # from there
        message = {
           "header": {
                      "messageKind": "queryRequest",
                      "sender": self.function_name,
                      "seedId": header["seedId"],
                      "previousEventId": context.event_id,
           },
           "body": {
                    # Trigger name should be named the same as the database query it trigger
                    "triggerName": self.name,
                    "queryParameters": query_parameters,
                    "writeTransaction": self.write_transaction,
                    "resultsMode": self.results_mode,
                    "splitResults": self.split_results,
                    "publishResultsTo": self.publish_results_to
           }
        }
        return([(self.recipient_topic, message)])

    def require_node_has_property(self, key, value=None):
        self.node_required_property[key] = value

    def require_body_has_property(self, key, value=None):
        self.body_required_property[key] = value

    def require_node_property_type(self, key, value):
        # Require value of a key-value pair to be an instance of a particular type
        self.node_required_property_type[key] = value

    def require_trellis_config(self, key, value):
        # Get keys or key-value pairs from the Trellis config file
        self.trellis_required_property[key] = value

    def do_not_allow_node_labels(self, labels):
        ## TODO: Check that node of the banned labels are in the required labels
        self.banned_node_labels.extend(labels)

    def _get_query_parameters(self):
        query_parameters = {}
        for variable_name, node_key in self.query_variables.items():
            query_parameters[variable_name] = node[node_key]
        return(query_parameters)


class TrellisMessage(object):

    def __init__(self, *, message_kind=None, sender=None, seed_id=None, previous_event_id=None):
        self.message_kind = message_kind
        self.sender = sender
        self.seed_id = seed_id
        self.previous_event_id = previous_event_id

        self.context = None
        self.header = None
        self.body = None

    """
    def parse_query_result(event, context)
        Parse Trellis messages from Pub/Sub event & context.

        Args:
            event (type):
            context (type):

        Message format:
            - context
                - event_id (required)
            - event
                - header
                    - sentFrom (required)
                    - method (optional)
                    - resource (optional)
                    - labels (optional)
                    - seedId (optional)
                    - previousEventId (optional)
                - body
                    - cypher (optional)
                    - results (optional)
        
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)
        
        self.context = context
        self.header = data['header']
        self.body = data['body']

        self.event_id = context.event_id
        self.seed_id = self.header.get('seedId')
        
        # If no seed specified, assume this is the seed event
        if not self.seed_id:
            self.seed_id = self.event_id

        self.results = {}
        if self.body.get('results'):
            self.results = self.body.get('results')

        self.node = None
        if self.results.get('node'):
            self.node = self.results['node']
    """

    def parse_pubsub_message(self, event, context):
        pubsub_message = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(pubsub_message)

        # Not sure what cases caused me to add this to db-query,
        # but keeping it in because I don't see any harm
        if type(data) == str:
            logging.warn("Message data not correctly loaded as JSON. " +
                         "Used eval to convert from string.")
            data = eval(data)
        
        self.context = context
        self.header = data['header']
        self.body = data['body']
        self.sender = self.header['sender']
        
        # The event_id is created by Pub/Sub or the message broker
        # so only get it when parsing messages
        self.event_id = int(context.event_id)
        self.seed_id = int(self.header.get('seedId'))

        
        # If no seed specified, assume this is the seed event
        if not self.seed_id:
            self.seed_id = self.event_id

    def format_json_message(self):
        raise NotImplementedError


class QueryRequest(TrellisMessage):

    def __init__(self, 
                 sender=None, 
                 seed_id=None, 
                 previous_event_id=None,
                 * , # Begin keyword-only arguments
                 query_name=None,
                 query_parameters={}, 
                 write_transaction = False, 
                 results_mode = "data", 
                 split_results = True, 
                 publish_results_to = []):
        
        super().__init__(
                         message_kind="queryRequest",
                         sender=sender, 
                         seed_id=seed_id, 
                         previous_event_id=previous_event_id)
        
        self.query_name = query_name
        self.query_parameters = query_parameters
        self.write_transaction = write_transaction
        self.results_mode = results_mode
        self.split_results = split_results
        self.publish_results_to = publish_results_to

    def parse_pubsub_message(self, event, context):
        super().parse_pubsub_message(event, context)

        if self.header['messageKind'] != "queryRequest":
            return ValueError

        # Required fields
        self.previous_event_id = int(self.header['previousEventId'])
        self.trigger_name = self.body['triggerName']
        self.write_transaction = self.body['writeTransaction']
        self.results_mode = self.body['resultsMode']
        self.split_results = self.body['splitResults']

        # Optional fields
        self.query_parameters = self.body.get('queryParameters')
        self.publish_results_to = self.body.get('publishResultsTo')

        if not isinstance(self.publish_results_to, list):
            return ValueError

    def format_json_message(self):
        message = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           },
           "body": {
                    # Trigger name should be named the same as the database query it trigger
                    "queryName": self.query_name,
                    "queryParameters": self.query_parameters,
                    "writeTransaction": self.write_transaction,
                    "resultsMode": self.results_mode,
                    "splitResults": self.split_results,
                    "publishResultsTo": self.publish_results_to
           }
        }
        #return([(self.recipient_topic, message)])
        return message


class QueryResponse(TrellisMessage):

    def __init__(self,
                 sender,
                 seed_id,
                 previous_event_id,
                 *, # Begin keyword-only arguments
                 query_name,
                 graph,
                 result_summary):

        if not isinstance(graph, neo4j.graph.Graph):
            return TypeError
        if not isinstance(result_summary, neo4j.ResultSummary):
            return TypeError

        super().__init__(
                         message_kind = "queryResponse",
                         sender = sender,
                         seed_id = seed_id,
                         previous_event_id = previous_event_id)

        self.query_name = query_name
        self.graph = graph
        self.result_summary = result_summary

        self.nodes = []
        for node in self.graph.nodes:
            self.nodes.append(node)

        self.relationships = []
        for relationship in self.graph.relationships:
            self.relationships.append(relationship)

    def parse_pubsub_message(self, event, context):
        super().parse_pubsub_message(event, context)

        if self.header['messageKind'] != "queryResponse":
            return ValueError

        # Required fields
        self.previous_event_id = int(self.header['previousEventId'])
        self.query_name = self.body['queryName']

        # Optional fields
        self.query_parameters = self.body.get('queryParameters')
        self.nodes = self.body.get('nodes')
        self.relationships = self.body.get('relationships')
        self.result_summary = self.body.get('resultSummary')

    def format_json_message(self):
        # I think I might need to massage the nodes and relationships objects
        # for transmitting.
        #result_summary_dict = _get_summary_result_dict(self.result_summary)


        message = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           },
           "body": {
                    "queryName": self.query_name,
                    "nodes": [self._get_node_dict(node) for node in self.nodes],
                    "relationships": [self._get_relationship_dict(rel) for rel in self.relationships],
                    "resultSummary": self._get_result_summary_dict(self.result_summary)
           }
        }
        return message

    def format_json_message_iter(self):
        for entity in self.nodes + self.relationships:
            yield self._format_json_message_single_result(entity)

    def _format_json_message_single_result(self, entity):
        result_nodes = None
        result_relationship = None

        if isinstance(entity, neo4j.graph.Node):
            node = [entity]
            relationship = []
        elif isinstance(entity, neo4j.graph.Relationship):
            node = []
            relationship = [entity]

        result_summary_dict = self._get_result_summary_dict(self.result_summary)

        message = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           },
           "body": {
                    "queryName": self.query_name,
                    "nodes": node,
                    "relationships": relationship,
                    "resultSummary": self._get_result_summary_dict(self.result_summary)
           }
        }
        return message

    def _get_result_summary_dict(self, result_summary):
        # Create a copy of the dict so that metadata and server
        # elements are preserved in self.result_summary.
        # ResultSummary source: https://github.com/neo4j/neo4j-python-driver/blob/4.4/neo4j/work/summary.py
        summary_dict = dict(self.result_summary.__dict__)
        # The metadata field contains the dictionary of information 
        # that is parsed into attributes of the ResultSummary object.
        del summary_dict['metadata']
        # The server value is an instance of the Server class and
        # its attributes contain more classes which would require
        # parsing to text and I'm not that concerned with the
        # information anyway.
        del summary_dict['server']

        summary_dict['counters'] = summary_dict['counters'].__dict__

        return summary_dict

    def _get_relationship_dict(self, relationship):
        """Convert neo4j.graph.Relationship object into dictionary.

        neo4j.graph.Relationship source: https://github.com/neo4j/neo4j-python-driver/blob/4.4/neo4j/graph/__init__.py

        Args: 
            relationship (neo4j.graph.Relationship): Object with relationship metadata.

        Returns: 
            relationship_dict (dict): Dictionary of metadata stored in Relationship object.
        """

        relationship_dict = {
            "id": relationship.id,
            "start_node": self._get_node_dict(relationship.start_node),
            "end_node": self._get_node_dict(relationship.end_node),
            "type": relationship.type,
            "properties": dict(relationship.items())
        }
        return relationship_dict

    def _get_node_dict(self, node):
        """Convert neo4j.graph.Node object into a dictionary.

        neo4j.graph.Node source: https://github.com/neo4j/neo4j-python-driver/blob/4.4/neo4j/graph/__init__.py

        Args:
            node (neo4j.graph.Node): Object with node metadata.

        Returns:
            node_dict (dict): Dictionary of metadata stored in the Node object.
        """

        node_dict = {
            "id": node.id,
            "labels": list(node.labels),
            "properties": dict(node.items())

        }
        return node_dict


class JobLauncherResponse(TrellisMessage):

    def __init__(self,
                 sender,
                 seed_id,
                 previous_event_id,
                 *,
                 job_properties):
        
        super().__init__(
                         message_kind = "jobLauncherResponse",
                         sender = sender,
                         seed_id = seed_id,
                         previous_event_id = previous_event_id)
        
        self.job_properties = job_properties

    def parse_pubsub_message(self, event, context):
        super().parse_pubsub_message(event, context)

        if self.header['messageKind'] != "jobLauncherResponse":
            return ValueError

        # Required fields
        self.job_properties = self.body['jobProperties']
    
    def format_json_message(self):
        message = {
           "header": {
                      "messageKind": self.message_kind,
                      "sender": self.sender,
                      "seedId": self.seed_id,
                      "previousEventId": self.previous_event_id,
           },
           "body": {
                    "jobProperties": self.job_properties
           }
        }
        return message


def make_unique_task_id(nodes, datetime_stamp):
    # Create pretty-unique hash value based on input nodes
    # https://www.geeksforgeeks.org/ways-sort-list-dictionaries-values-python-using-lambda-function/
    sorted_nodes = sorted(nodes, key = lambda i: i['id'])
    nodes_str = json.dumps(sorted_nodes, sort_keys=True, ensure_ascii=True, default=str)
    nodes_hash = hashlib.sha256(nodes_str.encode('utf-8')).hexdigest()
    print(nodes_hash)
    trunc_nodes_hash = str(nodes_hash)[:8]
    task_id = f"{datetime_stamp}-{trunc_nodes_hash}"
    return(task_id, trunc_nodes_hash)

def publish_to_pubsub_topic(publisher, project_id,  topic, message):
    """Convert dictionary to JSON and publish to Pub/Sub topic.

    Args:
        publisher (pubsub.PublisherClient): Pub/Sub client
        project_id (str): Google Cloud Project ID
        topic (str): Pub/Sub topic name
        message (dict): Dictionary with header and body fields.

    Returns:
        result (???)
    """

    topic_path = publisher.topic_path(project_id, topic)
    # https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable/36142844#36142844
    json_message = json.dumps(message, indent=4, sort_keys=True, default=str).encode('utf-8')
    result = PUBLISHER.publish(topic_path, data=json_message).result()
    return result