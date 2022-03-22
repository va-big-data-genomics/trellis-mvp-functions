import os
import pdb
import sys
import json
import math
import time
import yaml
import base64
import neobolt

from datetime import datetime

import neo4j
from neo4j import GraphDatabase

from urllib3.exceptions import ProtocolError
from neobolt.exceptions import ServiceUnavailable

from google.cloud import pubsub
from google.cloud import storage

import trellisdata as trellis
from trellisdata import DatabaseQuery

# Get runtime variables from cloud storage bucket
# https://www.sethvargo.com/secrets-in-serverless/
ENVIRONMENT = os.environ.get('ENVIRONMENT')
if ENVIRONMENT == 'google-cloud':

    # set up the Google Cloud Logging python client library
    # source: https://cloud.google.com/blog/products/devops-sre/google-cloud-logging-python-client-library-v3-0-0-release
    import google.cloud.logging
    client = google.cloud.logging.Client()
    client.setup_logging()

    # use Python's standard logging library to send logs to GCP
    import logging

    FUNCTION_NAME = os.environ['FUNCTION_NAME']

    vars_blob = storage.Client() \
                .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                .get_blob(os.environ['CREDENTIALS_BLOB']) \
                .download_as_string()
    parsed_vars = yaml.load(vars_blob, Loader=yaml.Loader)

    # Runtime variables
    #DATA_GROUP = parsed_vars['DATA_GROUP']
    PROJECT_ID = parsed_vars['GOOGLE_CLOUD_PROJECT']
    DB_QUERY_TOPIC = parsed_vars['DB_QUERY_TOPIC']
    DB_STORED_QUERIES_FILE = parsed_vars['DB_STORED_QUERIES_FILE']

    #NEO4J_URL = parsed_vars['NEO4J_URL']
    NEO4J_SCHEME = parsed_vars['NEO4J_SCHEME']
    NEO4J_HOST = parsed_vars['NEO4J_HOST']
    NEO4J_PORT = parsed_vars['NEO4J_PORT']
    NEO4J_USER = parsed_vars['NEO4J_USER']
    NEO4J_PASSPHRASE = parsed_vars['NEO4J_PASSPHRASE']
    #NEO4J_MAX_CONN = parsed_vars['NEO4J_MAX_CONN']

    # Pubsub client
    PUBLISHER = pubsub.PublisherClient()

    # Need to pull this from GCS
    queries_document = storage.Client() \
                        .get_bucket(os.environ['CREDENTIALS_BUCKET']) \
                        .get_blob(os.environ[DB_STORED_QUERIES]) \
                        .download_as_string()
    queries = yaml.load_all(queries_document, Loader=yaml.FullLoader)
    
    QUERY_DICT = {}
    for query in queries:
        QUERY_DICT[query.name] = query

    # Use Neo4j driver object to establish connections to the Neo4j
    # database and manage connection pool used by neo4j.Session objects
    # https://neo4j.com/docs/api/python-driver/current/api.html#driver
    DRIVER = GraphDatabase.driver(
        f"{NEO4J_SCHEME}://{NEO4J_HOST}:{NEO4J_PORT}",
        auth=("neo4j", NEO4J_PASSPHRASE),
        max_connection_pool_size=10)
else:
    FUNCTION_NAME = 'db-query-local'
    local_queries = "sample-queries.yaml"

    # Load database queries into a dictionary
    with open(local_queries, "r") as file_handle:
        queries = yaml.load_all(file_handle, Loader=yaml.FullLoader)
        QUERY_DICT = {}
        for query in queries:
            QUERY_DICT[query.name] = query

# Retrieve stored database queries from JSON
#with open(DB_STORED_QUERIES_FILE, 'r') as file_handle:
#    DB_STORED_QUERIES = json.load(file_handle)

QUERY_ELAPSED_MAX = 0.300
PUBSUB_ELAPSED_MAX = 10

"""
def republish_message(topic, data):

    max_retries = 3

    header = data["header"]
    counter = header.get("retry-count")
    if counter:
        if counter >= max_retries:
            raise ValueError(f"Function exceeded {max_retries} retries.")
        else:
            header["retry-count"] += 1
    else:
        header["retry-count"] = 1
    result = trellis.publish_to_topic(PUBLISHER, PROJECT_ID, DB_QUERY_TOPIC, data)
    return result
"""

def query_database(write_transaction, driver, query, query_parameters):
    """Run a Cypher query against the Neo4j database.

    Args:
        write_transaction (bool): Indicate whether write permission is needed.
        driver (neo4j.Driver): Official Neo4j Python driver
        query (str): Parameterized Cypher query
        query_parameters (dict): Parameters values that will be used in the query.
    Returns:
        neo4j.graph.Graph: https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.graph.Graph
        neo4j.ResultSummary: https://neo4j.com/docs/api/python-driver/current/api.html#resultsummary
    """

    with driver.session() as session:
        if write_transaction:
            graph, result_summary = session.write_transaction(_stored_procedure_transaction_function, query, **query_parameters)
        else:
            graph, result_summary = session.read_transaction(_stored_procedure_transaction_function, query, **query_parameters)
    return graph, result_summary

def _stored_procedure_transaction_function(tx, query, **query_parameters):
    result = tx.run(query, query_parameters)
    # Return graph data and ResultSummary object
    return result.graph(), result.consume()

def db_query(event, context, local_driver=None):
    """When an object node is added to the database, launch any
       jobs corresponding to that node label.

       Args:
            event (dict): Event payload.
            context (google.cloud.functions.Context): Metadata for the event.
    """

    start = datetime.now()
    query_request = trellis.QueryRequestReader(
                                               event=event, 
                                               context=context)
    #query_request.parse_pubsub_message(event, context)
    
    print(f"> Received message (context): {query_request.context}.")
    print(f"> Message header: {query_request.header}.")
    print(f"> Message body: {query_request.body}.")

    if ENVIRONMENT == 'google-cloud':
        # Time from message publication to reception
        request_publication_time = trellis.utils.convert_timestamp_to_rfc_3339(query_request.context.timestamp)
        publish_elapsed = datetime.now() - request_publication_time
        if publish_elapsed.total_seconds() > PUBSUB_ELAPSED_MAX:
            print(
                  f"> Time to receive message ({int(publish_elapsed.total_seconds())}) " +
                  f"exceeded {PUBSUB_ELAPSED_MAX} seconds after publication.")
    elif local_driver:
        DRIVER = local_driver

    # TODO: maybe I should store custom queries after they've been
    # created and give them unique IDs based on content so I can 
    # lookup already used queries
    if query_request.custom == True:
        # If custom query has been provided, create a new
        # instance of the DatabaseQuery class
        parameterized_query = DatabaseQuery(
            name=query_request.query_name,
            query=query_request.query,
            parameters={},
            write_transaction=query_request.write_transaction,
            publish_to=query_request.publish_to)
    else:
        parameterized_query = QUERY_DICT[query_request.query_name]

    try:
        query_start = time.time()
        graph, result_summary = query_database(
            #write_transaction = query_request.write_transaction,
            write_transaction = parameterized_query.write_transaction,
            driver = DRIVER,
            query = parameterized_query.query,
            query_parameters = query_request.query_parameters)
        query_elapsed = time.time() - query_start
        print(f"> Query elapsed walltime: {query_elapsed:.3f} seconds. " +
              f"Available after: {result_summary.result_available_after} ms." +
              f"Consumed after: {result_summary.result_consumed_after} ms.")
        #print(f"> Elapsed time to run query: {query_elapsed:.3f}. Query: {query}.")
        if query_elapsed > QUERY_ELAPSED_MAX:
            print(f"> Time to run query ({query_elapsed:.3f}) exceeded {QUERY_ELAPSED_MAX:.3f}. Query: {query}.")
    # Neo4j http connector
    except ProtocolError as error:
        logging.error(f"> Encountered Protocol Error: {error}.")
        # Add message back to queue
        #result = republish_message(DB_QUERY_TOPIC, data)
        #logging.warn(f"> Published message to {DB_QUERY_TOPIC} with result: {result}.")
        # Duplicate message flagged as warning
        #logging.warn(f"> Encountered Protocol Error: {error}.")
        return
    except ServiceUnavailable as error:
        logging.error(f"> Encountered Service Interrupion: {error}.")
        # Remove this connection(?) - causes UnboundLocalError
        #GRAPH = None
        # Add message back to queue
        #result = republish_message(DB_QUERY_TOPIC, data)
        #logging.warn(f"> Published message to {DB_QUERY_TOPIC} with result: {result}.")
        # Duplicate message flagged as warning
        #logging.warn(f"> Requeued message: {pubsub_message}.")
        return
    except ConnectionResetError as error:
        logging.error(f"> Encountered connection interruption: {error}.")
        # Add message back to queue
        #result = republish_message(DB_QUERY_TOPIC, data)
        #logging.warn(f"> Published message to {DB_QUERY_TOPIC} with result: {result}.")
        # Duplicate message flagged as warning
        #logging.warn(f"> Requeued message: {pubsub_message}.")
        return

    query_response = trellis.QueryResponseWriter(
        sender = FUNCTION_NAME,
        seed_id = query_request.seed_id,
        previous_event_id = query_request.event_id,
        query_name = query_request.query_name,
        graph = graph,
        result_summary = result_summary)

    # Return if no pubsub topic or not running on GCP
    if not parameterized_query.publish_to or not ENVIRONMENT == 'google-cloud':
        print("No Pub/Sub topic specified; result not published.")

        # Execution time block
        end = datetime.now()
        execution_time = (end - start).seconds
        time_threshold = int(execution_time/10) * 10
        if time_threshold > 0:
            print(f"> Execution time exceeded {time_threshold} seconds.")
        return query_response
    else:
        # Track how many messages are published to each topic
        published_message_counts = {}
        for topic in query_request.publish_results_to:
            published_message_counts[topic] = 0

        for topic in query_request.publish_results_to:
            if query_request.split_results == 'True':
                """ I think we can always send a result because even if no
                    node or relationship is returned we may still want to
                    use the summary statistics.

                if not query_results:
                    # If no results; send one message so triggers can respond to null
                    query_result = {}
                    message = format_pubsub_message(
                                                    method = method,
                                                    labels = labels,
                                                    query = query,
                                                    results = query_result,
                                                    seed_id = seed_id,
                                                    event_id = event_id,
                                                    retry_count=retry_count)
                    print(f"> Pubsub message: {message}.")
                    publish_result = publish_to_topic(topic, message)
                    print(f"> Published message to {topic} with result: {publish_result}.")
                    published_message_counts[topic] += 1
                """
                for message in query_response.format_json_message_iter():
                    print(f"> Pubsub message: {message}.")
                    publish_result = trellis.utils.publish_to_pubsub_topic(topic, message)
                    print(f"> Published message to {topic} with result: {publish_result}.")
                    published_message_counts[topic] += 1
            else:
                message = query_response.format_json_message()
                print(f"> Pubsub message: {message}.")
                publish_result = trellis.utils.publish_to_pubsub_topic(topic, message)
                print(f"> Published message to {topic} with result: {publish_result}.")
                published_message_counts[topic] += 1
        logging.info(f"> Summary of published messages: {published_message_counts}")

    # Execution time block
    end = datetime.now()
    execution_time = (end - start).seconds
    time_threshold = int(execution_time/10) * 10
    if time_threshold > 0:
        print(f"> Execution time exceeded {time_threshold} seconds.")
