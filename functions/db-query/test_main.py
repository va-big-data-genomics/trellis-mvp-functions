#!/usr/bin/env python3

import json
import mock
import neo4j
import base64
import pytest

from unittest import TestCase

import main

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

class TestQueryDatabase(TestCase):
	bolt_port = 7687
	bolt_address = "localhost"
	bolt_uri = f"bolt://{bolt_address}:{bolt_port}"
	
	user = "neo4j"
	password = "test"
	auth_token = (user, password)

	driver = neo4j.GraphDatabase.driver(bolt_uri, auth=auth_token)

	query_params = {
				"instanceId": "6433280663749256939",
				"instanceName": "google-pipelines-worker-6c3d415be62e2ebf2774924ced0fd771",
				"stopTime": "2021-11-04T23:16:53.95614Z",
				"stopTimeEpoch": "1636067813.95614",
				"stoppedBy": "genomics-api",
				"status": "STOPPED",
	}

	create_query = f"CREATE (node:Job {{instanceId: \"{query_params['instanceId']}\", instanceName: \"{query_params['instanceName']}\" }}) RETURN node"
	update_query = "MATCH (node:Job {instanceId: $instanceId, instanceName: $instanceName } ) SET node.stopTime = $stopTime, node.stopTimeEpoch = $stopTimeEpoch, node.stoppedBy = $stoppedBy, node.status = $status, node.durationMinutes = duration.inSeconds(datetime(node.startTime), datetime(node.stopTime)).minutes RETURN node"
	del_query = f"MATCH (node:Job {{instanceId: \"{query_params['instanceId']}\", instanceName: \"{query_params['instanceName']}\" }}) DELETE node"

	def test_query_database(cls):
		write_transaction = True

		with cls.driver.session() as session:
			result = session.run(cls.create_query)

		graph, result_summary = main.query_database(write_transaction, cls.driver, cls.update_query, cls.query_params)
		
		# Parse the results to make sure they return the correct things
		nodes = [node for node in graph.nodes]
		assert len(nodes) == 1
		
		node = nodes[0]
		assert node['instanceId'] == cls.query_params['instanceId']
		assert node['stopTimeEpoch'] == cls.query_params['stopTimeEpoch']
		assert list(node.labels) == ['Job']

		# Delete job node
		with cls.driver.session() as session:
			result = session.run(cls.del_query)


class TestDbQuery(TestCase):

	bolt_port = 7687
	bolt_address = "localhost"
	bolt_uri = f"bolt://{bolt_address}:{bolt_port}"
	
	user = "neo4j"
	password = "test"
	auth_token = (user, password)

	driver = neo4j.GraphDatabase.driver(bolt_uri, auth=auth_token)

	# Trellis attributes
	sender = "test-messages"
	seed_id = 123
	previous_event_id = 456

	mock_context = mock.Mock()
	mock_context.event_id = '617187464135194'
	mock_context.timestamp = '2019-07-15T22:09:03.761Z'

	def test_query_request(cls):
		data = {
			'header': {
				'messageKind': 'queryRequest',  
				'sender': 'trellis-log-delete-instance', 
				'seedId': '3329816530980893', 
				'previousEventId': '3329816530980893', 
			}, 
			'body': {
				'queryName': 'SetJobStopped',
				'queryParameters': {
					"instanceId": "6433280663749256939",
					"instanceName": "google-pipelines-worker-6c3d415be62e2ebf2774924ced0fd771",
					"stopTime": "2021-11-04T23:16:53.95614Z",
					"stopTimeEpoch": "1636067813.95614",
					"stoppedBy": "genomics-api",
					"status": "STOPPED",
				},
				'writeTransaction': True,
				'resultsMode': 'data',
				'splitResults': True,
				'publishResultsTo': ['check-triggers']
			}
		}

		data_str = json.dumps(data)
		data_utf8 = data_str.encode('utf-8')
		event = {'data': base64.b64encode(data_utf8)}

		# Create job node
		query_params = data['body']['queryParameters']
		create_query = f"CREATE (node:Job {{instanceId: \"{query_params['instanceId']}\", instanceName: \"{query_params['instanceName']}\" }}) RETURN node"
		with cls.driver.session() as session:
			result = session.run(create_query)

		main.db_query(event, cls.mock_context, cls.driver)

		# Delete job node
		del_query = f"MATCH (node:Job {{instanceId: \"{query_params['instanceId']}\", instanceName: \"{query_params['instanceName']}\" }}) DELETE node"
		with cls.driver.session() as session:
			result = session.run(del_query)