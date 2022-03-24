import os
import re
import pdb
import json
import uuid
import yaml
import iso8601
import importlib

import trellisdata as trellis

from anytree import Node, RenderTree
from anytree.search import find
from collections import deque

from google.cloud import storage
from google.cloud import pubsub

class Struct:
    # https://stackoverflow.com/questions/6866600/how-to-parse-read-a-yaml-file-into-a-python-object
    def __init__(self, **entries):
        self.__dict__.update(entries)


class TaxonomyParser:
    """
    This class is a wrapper on a Tree class from the anytree library to hold
    different data hierarchies read from a JSON representation

    Source: https://towardsdatascience.com/represent-hierarchical-data-in-python-cd36ada5c71a
    """

    def __init__(self, level_prefix = "L"):
        self.prefix = level_prefix
        self.nodes = {}
        self.root_key = None

    def find_by_name(self, name) -> Node:
        """
        Retrieve a node by its unique identifier name
        """
        root = self.nodes[self.root_key]
        node = find(root, lambda node: node.name == name)
        return node
   
    def read_from_json(self, fname):
        """
        Read the taxonomy from a JSON file given as input
        """
        
        self.nodes = {}
        try:
            with open(fname, "r") as f:
                data = json.load(f)
                n_levels = len(list(data.keys()))

                # read the root node
                root = data[f"{self.prefix}0"][0]
                name = root["name"]
                _ = root.pop("name")
                
                self.nodes[name] = Node(name, **root)
                self.root_key = name

                # populate the tree
                for k in range(1, n_levels):
                    
                    key = f"{self.prefix}{k}"
                    nodes = data[key]

                    for n in nodes:
                        try:
                            assert "name" in n
                            name = n["name"]
                            _ = n.pop("name")
                            parent = n["parent"]
                            _ = n.pop("parent")
                            
                            self.nodes[name] = Node(
                                name,
                                parent=self.nodes[parent],
                                **n
                            )
                        except AssertionError:
                            print(f"Malformed node representation: {n}")
                        except KeyError:
                            print(f"Detected a dangling node: {n['name']}")

        except (FileNotFoundError, KeyError):
            raise Exception("Not existent or malformed input JSON file")

    def read_from_string(self, data_str):
        """
        Read the taxonomy from a JSON file given as input
        """
        
        self.nodes = {}
        try:
            data = json.loads(data_str)
            n_levels = len(list(data.keys()))

            # read the root node
            root = data[f"{self.prefix}0"][0]
            name = root["name"]
            _ = root.pop("name")
            
            self.nodes[name] = Node(name, **root)
            self.root_key = name

            # populate the tree
            for k in range(1, n_levels):
                
                key = f"{self.prefix}{k}"
                nodes = data[key]

                for n in nodes:
                    try:
                        assert "name" in n
                        name = n["name"]
                        _ = n.pop("name")
                        parent = n["parent"]
                        _ = n.pop("parent")
                        
                        self.nodes[name] = Node(
                            name,
                            parent=self.nodes[parent],
                            **n
                        )
                    except AssertionError:
                        print(f"Malformed node representation: {n}")
                    except KeyError:
                        print(f"Detected a dangling node: {n['name']}")

        except (KeyError):
            raise Exception("Malformed input JSON string")


ENVIRONMENT = os.environ.get('ENVIRONMENT', '')
if ENVIRONMENT == 'google-cloud':

    # set up the Google Cloud Logging python client library
    # source: https://cloud.google.com/blog/products/devops-sre/google-cloud-logging-python-client-library-v3-0-0-release
    import google.cloud.logging
    client = google.cloud.logging.Client()
    client.setup_logging()

    # use Python's standard logging library to send logs to GCP
    import logging

    FUNCTION_NAME = os.environ['FUNCTION_NAME']
    TRIGGER_OPERATION = os.environ['TRIGGER_OPERATION']
    GIT_COMMIT_HASH = os.environ['GIT_COMMIT_HASH']
    GIT_VERSION_TAG = os.environ['GIT_VERSION_TAG']
    
    config_doc = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    #parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)
    # https://stackoverflow.com/questions/6866600/how-to-parse-read-a-yaml-file-into-a-python-object
    TRELLIS = yaml.safe_load(config_doc)
    #TRELLIS = Struct(**parsed_vars)

    PUBLISHER = pubsub.PublisherClient()
    STORAGE_CLIENT = storage.Client()

    # Need to pull this from GCS
    label_taxonomy = storage.Client() \
                        .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                        .get_blob(TRELLIS['LABEL_TAXONOMY']) \
                        .download_as_string()

    TAXONOMY_PARSER = TaxonomyParser()
    TAXONOMY_PARSER.read_from_string(label_taxonomy)
else:
    import logging

    TAXONOMY_PARSER = TaxonomyParser()
    TAXONOMY_PARSER.read_from_json('label-taxonomy.json')


def clean_metadata_dict(raw_dict):
    """Remove dict entries where the value is of type dict"""
    clean_dict = dict(raw_dict)

    uuid = clean_dict['metadata'].get('trellis-uuid')
    clean_dict['trellis-uuid'] = uuid

    # What if I just convert to strings
    # Remove values that are dicts
    delete_keys = []
    for key, value in clean_dict.items():
        if isinstance(value, dict):
            #del clean_dict[key]
            #delete_keys.append(key)
            clean_dict[key] = str(value)

    #for key in delete_keys:
    #    del clean_dict[key]

    # Convert size field from str to int
    clean_dict['size'] = int(clean_dict['size'])

    return clean_dict

def get_name_fields(event_name, event_bucket, commit_hash, version_tag):
    """(pbilling 200226): This should probably be moved to config file.

    Example input:
        event_name: va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz
        event_bucket: gcp-bucket-mvp-test-from-personalis
    
    Example output:
        path: va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz
        dirname: va_mvp_phase2/PLATE0/SAMPLE0/FASTQ
        basename: SAMPLE0_0_R1.fastq.gz
        name: SAMPLE0_0_R1
        extension: fastq.gz
        filetype: gz
        uri: gs://gcp-bucket-mvp-test-from-personalis/va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz
    """
    path_elements = event_name.split('/')
    name_elements = path_elements[-1].split('.')
    name_fields = {
                   "path": event_name,
                   "dirname": '/'.join(path_elements[:-1]),
                   "basename": path_elements[-1],
                   "name": name_elements[0],
                   "extension": '.'.join(name_elements[1:]),
                   "filetype": name_elements[-1],
                   "gitCommitHash": commit_hash,
                   "gitVersionTag": version_tag,
                   "uri" : "gs://" + event_bucket + "/" + event_name,
    }
    return name_fields

def get_time_fields(event):
    """
    Args:
        event (dict): Metadata properties stored as strings
    Return
        (dict): Times in iso (str) and from-epoch (int) formats
    """

    # Google datetime format: https://tools.ietf.org/html/rfc3339
    # ISO 8601 standard format: https://en.wikipedia.org/wiki/ISO_8601
    datetime_created = iso8601.parse_date(event['timeCreated'])
    datetime_updated = iso8601.parse_date(event['updated'])


    time_created_epoch = trellis.utils.get_seconds_from_epoch(datetime_created)
    time_created_iso = datetime_created.isoformat()

    time_updated_epoch = trellis.utils.get_seconds_from_epoch(datetime_updated)
    time_updated_iso = datetime_updated.isoformat()

    time_fields = {
                   'timeCreatedEpoch': time_created_epoch,
                   'timeUpdatedEpoch': time_updated_epoch,
                   'timeCreatedIso': time_created_iso,
                   'timeUpdatedIso': time_updated_iso
    }
    return time_fields

""" DEPRECATED with 1.3.0: Now use dynamicparameterized queries
def format_node_merge_query(db_dict, dry_run=False):
    # Create label string
    tmp_labels = list(db_dict['labels'])
    tmp_labels.remove('Blob')
    labels_str = ':'.join(tmp_labels)

    # Create database ON CREATE string
    create_strings = []
    for key, value in db_dict.items():
        if isinstance(value, str):
            create_strings.append(f'node.{key} = "{value}"')
        else:
            create_strings.append(f'node.{key} = {value}')
    create_string = ', '.join(create_strings)

    # If node already exists in the database, only update the following
    # values of the node (SET command), if the values are provided
    merge_keys = [
                  'md5Hash',
                  'size',
                  'timeUpdatedEpoch',
                  'timeUpdatedIso',
                  'timeStorageClassUpdated',
                  'updated',
                  'id',
                  'crc32c',
                  'generation',
                  'storageClass',
                  # Following are specific to Checksum objects
                  'fastqCount',
                  'microarrayCount']

    merge_strings = []
    for key in merge_keys:
        value = db_dict.get(key)
        if value:
            if isinstance(value, str):
                merge_strings.append(f'node.{key} = "{value}"')
            else:
                merge_strings.append(f'node.{key} = {value}')
    merge_string = ', '.join(merge_strings)

    query = (
        f"MERGE (node:Blob:{labels_str} {{ " +
            #f'bucket: "{db_dict["bucket"]}", ' +
            #f'path: "{db_dict["path"]}" }}) ' +
            f'id: "{db_dict["id"]}" }}) ' +
        "ON CREATE SET node.nodeCreated = timestamp(), " +
            'node.nodeIteration = "initial", ' +
            f"{create_string} " +
        f"ON MATCH SET " +
            'node.nodeIteration = "merged", ' +
            f"{merge_string} " +
        "RETURN node")
    return query
"""

def create_parameterized_merge_query(label, query_parameters):
    # Create label string
    #tmp_labels = list(db_dict['labels'])
    #tmp_labels.remove('Blob')
    #labels_str = ':'.join(tmp_labels)
    #label = query_parameters['labels'][0]

    """
    # Create database ON CREATE string
    create_strings = []
    for key, value in db_dict.items():
        if isinstance(value, str):
            create_strings.append(f'node.{key} = "{value}"')
        else:
            create_strings.append(f'node.{key} = {value}')
    create_string = ', '.join(create_strings)
    """

    create_strings = []
    for key in query_parameters.keys():
        create_strings.append(f'node.{key} = ${key}')
    create_string = ', '.join(create_strings)

    # If node already exists in the database, only update the following
    # values of the node (SET command), if the values are provided
    merge_keys = [
                  'md5Hash',
                  'size',
                  'timeUpdatedEpoch',
                  'timeUpdatedIso',
                  'timeStorageClassUpdated',
                  'updated',
                  'id',
                  'crc32c',
                  'generation',
                  'storageClass',
                  # Following are specific to Checksum objects
                  'fastqCount',
                  'microarrayCount']

    #merge_strings = []
    #for key in merge_keys:
    #    value = db_dict.get(key)
    #    if value:
    #        if isinstance(value, str):
    #            merge_strings.append(f'node.{key} = "{value}"')
    #        else:
    #            merge_strings.append(f'node.{key} = {value}')
    #merge_string = ', '.join(merge_strings)

    merge_strings = []
    for key in merge_keys:
        value = query_parameters.get(key)
        if value:
            merge_strings.append(f'node.{key} = ${key}')
    merge_string = ', '.join(merge_strings)

    """
    parameterized_query = (
        f"MERGE (node:Blob:{label} {{ " +
            #f'bucket: "{db_dict["bucket"]}", ' +
            #f'path: "{db_dict["path"]}" }}) ' +
            f'id: "{db_dict["id"]}" }}) ' +
        "ON CREATE SET node.nodeCreated = timestamp(), " +
            'node.nodeIteration = "initial", ' +
            f"{create_string} " +
        f"ON MATCH SET " +
            'node.nodeIteration = "merged", ' +
            f"{merge_string} " +
        "RETURN node")
    return query
    """

    parameterized_query = (
        f"MERGE (node:{label} {{ uri: $uri, crc32c: $crc32c }}) " +
        "ON CREATE SET node.nodeCreated = timestamp(), " +
            "node.nodeIteration = 'initial', " +
            f"{create_string} " +
        "ON MATCH SET " +
            "node.nodeIteration = 'merged', " + 
            f"{merge_string} " +
        "RETURN node")
    return parameterized_query

def add_uuid_to_blob(bucket, path):
    """For a json object, get and return json data.

    Args:
        bucket (str): Name of Google Cloud Storage (GCS) bucket.
        path (str): Path to GCS object.
    Returns:
        Blob.metadata (dict)
    """
    metadata = {'trellis-uuid': uuid.uuid4()}

    storage_client = storage.Client()
    bucket = STORAGE_CLIENT.get_bucket(bucket)
    blob = bucket.get_blob(path)
    blob.metadata = metadata
    blob.patch()

    return blob.metadata

    logging.info("The metadata for the blob {} is {}".format(blob.name, blob.metadata))

def assign_labels(path, label_match_patterns):
    """Used for testing"""
    labels = []
    for label, patterns in label_match_patterns.items():
        for pattern in patterns:
            match = re.fullmatch(pattern, path)
            if match:
                labels.append(label)
    return labels

def assign_label_and_metadata(query_parameters, label_patterns, label_functions):
    #query_parameters['labels'] = []
    labels = []
    for label, patterns in label_patterns.items():
        for pattern in patterns:
            match = re.fullmatch(pattern, query_parameters['path'])
            if match:
                labels.append(label)
                #query_parameters['labels'].append(label)
                metadata_functions = label_functions.get(label)
                if metadata_functions:
                    for metadata_function in metadata_functions:
                        custom_fields = metadata_function(query_parameters, match.groupdict())
                        query_parameters.update(custom_fields)
                # Break after a single pattern per label has been matched
                # According to single-label mode, objects can't/shouldn't(?)
                # match more than one label.
                break
    return query_parameters, labels

def get_leaf_labels(labels, taxonomy_parser):
    # Get only the shallowest labels of a branch of the taxonomy that should be applied 
    # to the node. The point of the taxonomy is so that we can retain lineage information
    # without applying multiple labels to a node.
    common_parents = []
    for label in labels:
        node = taxonomy_parser.find_by_name(label)
        # https://docs.python.org/3/library/collections.html#collections.deque
        parents = deque(node.path)

        parents.popleft() # Remove the arbitrary root node
        parents.pop()     # Remove the current label
        common_parents.extend(parents)
    common_parents = set(common_parents) # Only keep unique nodes
    
    # If a label is a parent of another label, exclude it
    for label in labels:
        if label in [parent.name for parent in common_parents]:
            labels.remove(label)
    return labels

def create_node_query(event, context, test=False):
    """When object created in bucket, add metadata to database.
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """

    # Define global variables for local testing
    #type(ENVIRONMENT)
    if test == True:
        ENVIRONMENT = 'local'

        GIT_COMMIT_HASH = "mock-hash"
        GIT_VERSION_TAG = "v0.1"
        TRIGGER_OPERATION = "local-test"
        FUNCTION_NAME = "create-blob-node"

        TAXONOMY_PARSER = TaxonomyParser()
        TAXONOMY_PARSER.read_from_json('label-taxonomy.json')

    logging.info(f"> Processing new object event: {event['name']}.")
    logging.info(f"> Event: {event}.")
    logging.info(f"> Context: {context}.")

    seed_id = context.event_id

    # Trellis config data
    name = event['name']
    bucket_name = event['bucket']

    if ENVIRONMENT == 'google-cloud':
        # Use bucket name to determine which config file should be used
        # to parse object metadata.
        # (Module name does not include project prefix)
        pattern = f"{TRELLIS['GOOGLE_CLOUD_PROJECT']}-(?P<suffix>\w+(?:-\w+)+)"
        match = re.match(pattern, bucket_name)
        suffix = match['suffix']

        # TODO: Create a separate instance of create-blob-node for each bucket
        #   and include the module in the function deployment parameters,
        #   controlled by Terraform.
        # Import the config module that corresponds to event-trigger bucket
        node_module_name = f"{TRELLIS['DATA_GROUP']}.{suffix}.create-node-config"
        node_module = importlib.import_module(node_module_name)
    else:
        import test_create_node_config as node_module

    node_kinds = node_module.NodeKinds()
    label_patterns = node_kinds.match_patterns
    label_functions = node_kinds.label_functions

    # Create dict of metadata to add to database node
    gcp_metadata = event
    query_parameters = clean_metadata_dict(event)

    # Add standard fields
    name_fields = get_name_fields(
                    event_name = event['name'], 
                    event_bucket = event['bucket'],
                    commit_hash = GIT_COMMIT_HASH,
                    version_tag = GIT_VERSION_TAG)
    time_fields = get_time_fields(event)

    query_parameters.update(name_fields)
    query_parameters.update(time_fields)

    # Add trigger operation as metadata property
    query_parameters['triggerOperation'] = TRIGGER_OPERATION

    # Populate query_parameters with metadata about object
    query_parameters, labels = assign_labels_and_metadata(query_parameters, label_patterns, label_functions)
    """
    query_parameters['labels'] = []
    for label, patterns in label_patterns.items():
        for pattern in patterns:
            match = re.fullmatch(pattern, name)
            if match:
                query_parameters['labels'].append(label)
                label_functions = node_kinds.label_functions.get(label)
                if label_functions:
                    for function in label_functions:
                        custom_fields = function(query_parameters, match.groupdict())
                        query_parameters.update(custom_fields)
                # Break after a single pattern per label has been matched
                # Why?
                break
    """

    labels = get_leaf_labels(labels, TAXONOMY_PARSER)
    """
    # Get only the shallowest labels of a branch of the taxonomy that should be applied 
    # to the node. The point of the taxonomy is so that we can retain lineage information
    # without applying multiple labels to a node.
    common_parents = []
    for label in query_parameters['labels']:
        node = parser.find_by_name(label)
        parents = deque(node.path)

        parents.popleft() # Remove the arbitrary root node
        parents.pop()     # Remove the current label
        common_parents.extend(parents)
    common_parents = set(common_parents) # Only keep unique nodes
    
    # If a label is a parent of another label, exclude it
    for label in query_parameters['labels']:
        if label in [parent.name for parent in common_parents]:
            query_parameters['labels'].remove(label)
    """

    # Max (1) label per node to choose parameterized query
    if len(labels) > 1:
        raise ValueError
    else:
        label = labels[0]

    # TODO: Implement new logic for ignore log files
    # Ignore log files
    #log_labels = set(['Log', 'Stderr', 'Stdout'])
    #log_intersection = log_labels.intersection(db_dict['labels'])
    if label == 'Log':
    #if query_parameters['filetype'] in ['log', 'stderr', 'stdout']:
        logging.info(f"> This is a log file; ignoring.")
        return

    # TODO: Support passing label query generator

    # Generate UUID
    if not query_parameters['trellisUuid'] and ENVIRONMENT == 'google-cloud':
        uuid = add_uuid_to_blob(
                                query_parameters['bucket'], 
                                query_parameters['path'])
        logging.info("The metadata for the blob {} is {}".format(blob.name, blob.metadata))
        query_parameters['trellisUuid'] = blob.metadata['uuid']


    # Dynamically create parameterized query
    parameterized_query = create_parameterized_merge_query(label, query_parameters)

    """ TODO: Move to categorize-blob-node
    # Key, value pairs unique to db_dict are trellis metadata
    trellis_metadata = {}
    for key, value in db_dict.items():
        if not key in gcp_metadata.keys():
            trellis_metadata[key] = value
    """

    #print(f"> Generating database query for node: {db_dict}.")
    #db_query = format_node_merge_query(db_dict)
    #print(f"> Database query: \"{db_query}\".")

    query_request = trellis.QueryRequestWriter(
        sender = FUNCTION_NAME,
        seed_id = seed_id,
        previous_event_id = seed_id,
        query_name = f"merge{label}Node",
        query_parameters = query_parameters,
        custom = True,
        query = parameterized_query,
        write_transaction = True,
        publish_to = TRELLIS['TOPIC_TRIGGERS'],
        returns = {"node": "node"})
    message = query_request.format_json_message()
    logging.info(f"> Pubsub message: {message}.")
    if ENVIRONMENT == 'google-cloud':
        result = trellis.publish_to_pubsub_topic(TRELLIS['TOPIC_DB_QUERY'], message)
        logging.info(f"> Published message to {TRELLIS['TOPIC_DB_QUERY']} with result: {result}.")
    else:
        logging.warning("Could not determine environment. Message was not published.")
        return(message)