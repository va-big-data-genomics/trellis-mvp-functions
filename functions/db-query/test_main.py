#!/usr/bin/env python3

import pdb
import json
import mock
import yaml
import neo4j
import base64
import pytest

from unittest import TestCase

import trellisdata as trellis
#from trellisdata import DatabaseQuery
import main

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

""" 20220713: We are not testing with a live database anymore.
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

		assert result_summary.result_available_after
		assert result_summary.result_consumed_after

		# Delete job node
		with cls.driver.session() as session:
			result = session.run(cls.del_query)
"""

"""
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
				'queryName': 'setJobStopped',
				'queryParameters': {
					"instanceId": "6433280663749256939",
					"instanceName": "google-pipelines-worker-6c3d415be62e2ebf2774924ced0fd771",
					"stopTime": "2021-11-04T23:16:53.95614Z",
					"stopTimeEpoch": "1636067813.95614",
					"stoppedBy": "genomics-api",
					"status": "STOPPED",
				},
				'custom': False
				#'writeTransaction': True,
				#'resultsMode': 'data',
				#'splitResults': True,
				#'publishResultsTo': ['check-triggers']
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

		main.main(event, cls.mock_context, cls.driver)

		# Delete job node
		del_query = f"MATCH (node:Job {{instanceId: \"{query_params['instanceId']}\", instanceName: \"{query_params['instanceName']}\" }}) DELETE node"
		with cls.driver.session() as session:
			result = session.run(del_query)

	def test_custom_query_request(cls):
		data = {
				'header': {
						   'messageKind': 'queryRequest', 
						   'sender': 'trellis-create-blob-node', 
						   'seedId': 123, 
						   'previousEventId': 345
				}, 
				'body': {
					    'queryName': 'mergeFastqNode', 
					    'queryParameters': {
					    	'bucket': 'gcp-bucket-mvp-test-from-personalis', 
					    	'componentCount': 32, 
					    	'contentType': 'application/octet-stream', 
					    	'crc32c': 'ftNG8w==', 
					    	'customTime': '1970-01-01T00:00:00.000Z', 
					    	'etag': 'CJmAwYe83OcCEAs=', 
					    	'eventBasedHold': False, 
					    	'generation': '1582075915288601', 
					    	'id': 'gcp-project-test-from-personalis/va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz/1582075915288601', 
					    	'kind': 'storage#object', 
					    	'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gcp-project-test-from-personalis/o/va_mvp_phase2%2FPLATE0%2FSAMPLE0%2FFASTQ%2FSAMPLE0_0_R1.fastq.gz?generation=1582075915288601&alt=media', 
					    	'metadata': "{'gcf-update-metadata': '2696298877621712'}", 
					    	'metageneration': '11', 
					    	'name': 'SAMPLE0_0_R1', 
					    	'selfLink': 'https://www.googleapis.com/storage/v1/b/gcp-project-test-from-personalis/o/va_mvp_phase2%2FPLATE0%2FSAMPLE0%2FFASTQ%2FSAMPLE0_0_R1.fastq.gz', 
					    	'size': 5955984357, 
					    	'storageClass': 'REGIONAL', 
					    	'temporaryHold': False, 
					    	'timeCreated': '2020-02-19T01:31:55.288Z', 
					    	'timeStorageClassUpdated': '2020-02-19T01:31:55.288Z', 
					    	'updated': '2022-02-28T21:13:19.739Z', 
					    	'trellisUuid': 1234, 
					    	'path': 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz', 
					    	'dirname': 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ', 
					    	'basename': 'SAMPLE0_0_R1.fastq.gz', 
					    	'extension': 'fastq.gz', 
					    	'filetype': 'gz', 
					    	'gitCommitHash': 'abcd5', 
					    	'gitVersionTag': None, 
					    	'uri': 'gs://gcp-bucket-mvp-test-from-personalis/va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz', 
					    	'timeCreatedEpoch': 1582075915.288, 
					    	'timeUpdatedEpoch': 1646082799.739, 
					    	'timeCreatedIso': '2020-02-19T01:31:55.288000+00:00', 
					    	'timeUpdatedIso': '2022-02-28T21:13:19.739000+00:00', 
					    	'plate': 'PLATE0', 
					    	'sample': 'SAMPLE0', 
					    	'matePair': 1, 
					    	'readGroup': 0
					    }, 
			    		'custom': True, 
			    		'query': "MERGE (node:Fastq { uri: $uri, crc32c: $crc32c }) ON CREATE SET node.nodeCreated = timestamp(), node.nodeIteration = 'initial', node.bucket = $bucket, node.componentCount = $componentCount, node.contentType = $contentType, node.crc32c = $crc32c, node.customTime = $customTime, node.etag = $etag, node.eventBasedHold = $eventBasedHold, node.generation = $generation, node.id = $id, node.kind = $kind, node.mediaLink = $mediaLink, node.metadata = $metadata, node.metageneration = $metageneration, node.name = $name, node.selfLink = $selfLink, node.size = $size, node.storageClass = $storageClass, node.temporaryHold = $temporaryHold, node.timeCreated = $timeCreated, node.timeStorageClassUpdated = $timeStorageClassUpdated, node.updated = $updated, node.trellisUuid = $trellisUuid, node.path = $path, node.dirname = $dirname, node.basename = $basename, node.extension = $extension, node.filetype = $filetype, node.gitCommitHash = $gitCommitHash, node.gitVersionTag = $gitVersionTag, node.uri = $uri, node.timeCreatedEpoch = $timeCreatedEpoch, node.timeUpdatedEpoch = $timeUpdatedEpoch, node.timeCreatedIso = $timeCreatedIso, node.timeUpdatedIso = $timeUpdatedIso, node.plate = $plate, node.sample = $sample, node.matePair = $matePair, node.readGroup = $readGroup ON MATCH SET node.nodeIteration = 'merged', node.size = $size, node.timeUpdatedEpoch = $timeUpdatedEpoch, node.timeUpdatedIso = $timeUpdatedIso, node.timeStorageClassUpdated = $timeStorageClassUpdated, node.updated = $updated, node.id = $id, node.crc32c = $crc32c, node.generation = $generation, node.storageClass = $storageClass RETURN node", 
			    		'writeTransaction': True, 
			    		'publishTo': 'check-triggers', 
			    		'returns': {'node': 'node'}
			    }
		}
		data_str = json.dumps(data)
		data_utf8 = data_str.encode('utf-8')
		event = {'data': base64.b64encode(data_utf8)}

		# Delete existing Fastq node (if exists)
		delete_query = f"MATCH (node:Fastq) WHERE node.uri = $uri DETACH DELETE node"
		with cls.driver.session() as session:
			result = session.run(delete_query, uri=data['body']['queryParameters']['uri'])
			result_summary = result.consume()
		
		# Run custom query
		main.main(event, cls.mock_context, cls.driver)

		# Check that Fastq node has been created
		match_query = f"MATCH (node:Fastq) WHERE node.uri = $uri RETURN node"
		with cls.driver.session() as session:
			result = session.run(match_query, uri=data['body']['queryParameters']['uri'])
			graph = result.graph()
		nodes = list(graph.nodes)
		assert len(nodes) == 1
		node = nodes[0]
		node_labels = list(node.labels)
		node_properties = dict(node.items())
		
		assert node_properties['basename'] =='SAMPLE0_0_R1.fastq.gz'
"""

class TestLoadConfig(TestCase):
	config_file = "sample-config.yaml"
	queries_file = "sample-queries.yaml"

	def test_load_trellis_variables(cls):
		with open(cls.config_file, 'r') as config_document:
			config_map = yaml.safe_load(config_document)
		#TRELLIS = trellis.utils.Struct(**parsed_vars)

	def test_get_trellis_topic(cls):
		with open(cls.config_file, 'r') as config_document:
			config_map = yaml.safe_load(config_document)
		#TRELLIS = trellis.utils.Struct(**parsed_vars)

		db_query_topic = config_map["TOPIC_DB_QUERY"]
		assert db_query_topic == 'db-query'

	def test_get_trellis_topic_using_variable_(cls):
		with open(cls.config_file, 'r') as config_document:
			config_map = yaml.safe_load(config_document)
		#TRELLIS = trellis.utils.Struct(**parsed_vars)

		topic_reference = "TOPIC_DB_QUERY"

		db_query_topic = config_map[topic_reference]
		assert db_query_topic == 'db-query'


	def test_load_queries_from_file(cls):
		with open(cls.queries_file, 'r') as queries_document:
			queries = yaml.load_all(queries_document, Loader=yaml.FullLoader)
			queries = list(queries)

		for query in queries:
			assert isinstance(query, trellis.DatabaseQuery) == True