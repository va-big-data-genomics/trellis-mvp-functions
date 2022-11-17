#!/usr/bin/env python3

import neo4j
import trellisdata as trellis

bolt_port = 7687
bolt_address = "localhost"
bolt_uri = f"bolt://{bolt_address}:{bolt_port}"

user = "neo4j"
password = "test"
auth_token = (user, password)

# Make sure local instance of database is running
driver = neo4j.GraphDatabase.driver(bolt_uri, auth=auth_token)

query_document = "database-queries-pilot.yaml"
trigger_document =  "database-triggers-pilot.yaml"

grapher = trellis.OperationGrapher(
			query_document = query_document,
			trigger_document = trigger_document,
			neo4j_driver = driver)
query_nodes = grapher.add_query_nodes_to_neo4j()
trigger_nodes = grapher.add_trigger_nodes_to_neo4j()

grapher.connect_nodes()