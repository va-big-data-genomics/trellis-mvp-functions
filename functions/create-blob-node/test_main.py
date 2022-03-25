#!/usr/bin/env python3

import pdb
import json
import mock
import neo4j
import base64
import pytest

from unittest import TestCase

import main
import example_create_node_config as node_module

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

# Source: https://towardsdatascience.com/represent-hierarchical-data-in-python-cd36ada5c71a
class TestTaxonomyParser(TestCase):

	example_taxonomy = "flat_taxonomy_data.json"
	label_taxonomy_file = 'label-taxonomy.json'

	def test_example_read_from_json(cls):

		taxonomy_parser = main.TaxonomyParser()
		taxonomy_parser.read_from_json(cls.example_taxonomy)

		taxonomy_parser.find_by_name("Firewall")

	def test_label_read_from_json(cls):

		taxonomy_parser = main.TaxonomyParser()
		taxonomy_parser.read_from_json(cls.label_taxonomy_file)

		taxonomy_parser.find_by_name("PersonalisSequencing")


class TestCleanGcpEvent(TestCase):

	def test_clean_gcp_event_with_uuid(cls):

		event_with_uuid = {
			'bucket': 'gcp-project-test-from-personalis', 
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
			'metadata': {'gcf-update-metadata': '2696298877621712', 'trellis-uuid': '49a453c8-48e2-4ee7-b354-63aed2df0d00'}, 
			'metageneration': '11', 
			'name': 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz', 
			'selfLink': 'https://www.googleapis.com/storage/v1/b/gcp-project-test-from-personalis/o/va_mvp_phase2%2FPLATE0%2FSAMPLE0%2FFASTQ%2FSAMPLE0_0_R1.fastq.gz', 
			'size': '5955984357', 
			'storageClass': 'REGIONAL', 
			'temporaryHold': False, 
			'timeCreated': '2020-02-19T01:31:55.288Z', 
			'timeStorageClassUpdated': '2020-02-19T01:31:55.288Z', 
			'updated': '2022-02-28T21:13:19.739Z'}

		cleaned_dict = main.clean_metadata_dict(event_with_uuid)

		assert cleaned_dict.get('bucket')
		
		assert cleaned_dict['trellis-uuid'] == '49a453c8-48e2-4ee7-b354-63aed2df0d00'
		assert cleaned_dict['size'] ==  5955984357
		assert cleaned_dict['metadata'] == "{'gcf-update-metadata': '2696298877621712', 'trellis-uuid': '49a453c8-48e2-4ee7-b354-63aed2df0d00'}"

	def test_clean_gcp_event_without_uuid(cls):
		event_no_uuid = {
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
			'metadata': {'gcf-update-metadata': '2696298877621712'}, 
			'metageneration': '11', 
			'name': 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz', 
			'selfLink': 'https://www.googleapis.com/storage/v1/b/gcp-project-test-from-personalis/o/va_mvp_phase2%2FPLATE0%2FSAMPLE0%2FFASTQ%2FSAMPLE0_0_R1.fastq.gz', 
			'size': '5955984357', 
			'storageClass': 'REGIONAL', 
			'temporaryHold': False, 
			'timeCreated': '2020-02-19T01:31:55.288Z', 
			'timeStorageClassUpdated': '2020-02-19T01:31:55.288Z', 
			'updated': '2022-02-28T21:13:19.739Z'}

		cleaned_dict = main.clean_metadata_dict(event_no_uuid)

		assert cleaned_dict.get('bucket')
		
		assert 'trellis-uuid' in cleaned_dict.keys()
		assert cleaned_dict['trellis-uuid'] == None
		assert cleaned_dict['size'] ==  5955984357
		assert cleaned_dict['metadata'] == "{'gcf-update-metadata': '2696298877621712'}"


class TestGetNameFields(TestCase):

	def test_get_name_fields(cls):
		name = 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz'
		bucket = 'gcp-bucket-mvp-test-from-personalis'

		name_fields = main.get_name_fields(
						event_name = name, 
						event_bucket = bucket,
						commit_hash = "abcd5",
						version_tag = None)

		assert name_fields['path'] == 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz'
		assert name_fields['dirname'] == 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ'
		assert name_fields['basename'] == 'SAMPLE0_0_R1.fastq.gz'
		assert name_fields['name'] == 'SAMPLE0_0_R1'
		assert name_fields['extension'] == 'fastq.gz'
		assert name_fields['filetype'] == 'gz'
		assert name_fields['uri'] == f'gs://{bucket}/{name}'


class TestGetTimeFields(TestCase):

	event_no_uuid = {
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
		'metadata': {'gcf-update-metadata': '2696298877621712'}, 
		'metageneration': '11', 
		'name': 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz', 
		'selfLink': 'https://www.googleapis.com/storage/v1/b/gcp-project-test-from-personalis/o/va_mvp_phase2%2FPLATE0%2FSAMPLE0%2FFASTQ%2FSAMPLE0_0_R1.fastq.gz', 
		'size': '5955984357', 
		'storageClass': 'REGIONAL', 
		'temporaryHold': False, 
		'timeCreated': '2020-02-19T01:31:55.288Z', 
		'timeStorageClassUpdated': '2020-02-19T01:31:55.288Z', 
		'updated': '2022-02-28T21:13:19.739Z'}

	def test_get_time_fields(cls):

		time_fields = main.get_time_fields(cls.event_no_uuid)
		
		assert time_fields['timeCreatedEpoch'] == 1582075915.288
		assert time_fields['timeUpdatedEpoch'] == 1646082799.739
		assert time_fields['timeCreatedIso'] == '2020-02-19T01:31:55.288000+00:00'
		assert time_fields['timeUpdatedIso'] == '2022-02-28T21:13:19.739000+00:00'


class TestPopulateNodeMetadata(TestCase):
	# Hard to write tests for this because it relies on an 
	# external config file. Create a local config file
	# that just addresses a few use cases.

	import example_create_node_config as node_module

	node_kinds = node_module.NodeKinds()
	label_patterns = node_kinds.match_patterns
	label_functions = node_kinds.label_functions

	fastq_event_no_uuid = {
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
		'metadata': {'gcf-update-metadata': '2696298877621712'}, 
		'metageneration': '11', 
		'name': 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz', 
		'selfLink': 'https://www.googleapis.com/storage/v1/b/gcp-project-test-from-personalis/o/va_mvp_phase2%2FPLATE0%2FSAMPLE0%2FFASTQ%2FSAMPLE0_0_R1.fastq.gz', 
		'size': '5955984357', 
		'storageClass': 'REGIONAL', 
		'temporaryHold': False, 
		'timeCreated': '2020-02-19T01:31:55.288Z', 
		'timeStorageClassUpdated': '2020-02-19T01:31:55.288Z', 
		'updated': '2022-02-28T21:13:19.739Z'}

	def test_loading_label_patterns(cls):
		node_kinds = node_module.NodeKinds()
		label_patterns = node_kinds.match_patterns

		assert cls.label_patterns['Fastq'] == [r"^va_mvp_phase2\/(?P<plate>\w+)\/(?P<sample>\w+)\/FASTQ\/.*\.fastq\.gz$"]

	def test_label_assignment(cls):

		query_parameters = main.clean_metadata_dict(cls.fastq_event_no_uuid)

		name_fields = main.get_name_fields(
			event_name = cls.fastq_event_no_uuid['name'], 
			event_bucket = cls.fastq_event_no_uuid['bucket'],
			commit_hash = "abcd5",
			version_tag = None)
		time_fields = main.get_time_fields(cls.fastq_event_no_uuid)

		query_parameters.update(name_fields)
		query_parameters.update(time_fields)

		assert query_parameters['path'] == 'va_mvp_phase2/PLATE0/SAMPLE0/FASTQ/SAMPLE0_0_R1.fastq.gz'
		assert cls.label_patterns['Fastq'] == [r"^va_mvp_phase2\/(?P<plate>\w+)\/(?P<sample>\w+)\/FASTQ\/.*\.fastq\.gz$"]

		labels = main.assign_labels(
									query_parameters['path'], 
									cls.label_patterns, )
		assert labels == ['Blob', 'Fastq']

	def test_get_fastq_node_metadata(cls):
		node_kinds = node_module.NodeKinds()
		label_patterns = node_kinds.match_patterns

		query_parameters = main.clean_metadata_dict(cls.fastq_event_no_uuid)

		name_fields = main.get_name_fields(
			event_name = cls.fastq_event_no_uuid['name'], 
			event_bucket = cls.fastq_event_no_uuid['bucket'],
			commit_hash = "abcd5",
			version_tag = None)
		time_fields = main.get_time_fields(cls.fastq_event_no_uuid)


		query_parameters.update(name_fields)
		query_parameters.update(time_fields)

		query_parameters, labels = main.assign_label_and_metadata(
								query_parameters,
								cls.label_patterns,
								cls.label_functions)

		#pdb.set_trace()
		assert query_parameters.get('labels') == None
		assert query_parameters['plate'] == 'PLATE0'
		assert query_parameters['sample'] == 'SAMPLE0'
		assert query_parameters['matePair'] == 1
		assert query_parameters['readGroup'] == 0


class TestGetLeafLabel(TestCase):

	def test_get_fastq_label(cls):

		labels = ['Blob', 'Fastq']

		taxonomy_parser = main.TaxonomyParser()
		taxonomy_parser.read_from_json('label-taxonomy.json')

		leaf_labels = main.get_leaf_labels(labels, taxonomy_parser)

		assert leaf_labels == ['Fastq']


class TestCreateParameterizedMergeQuery(TestCase):

	fastq_label = 'Fastq'
	fastq_query_parameters = {
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
	}

	## Get query response
	bolt_port = 7687
	bolt_address = "localhost"
	bolt_uri = f"bolt://{bolt_address}:{bolt_port}"

	user = "neo4j"
	password = "test"
	auth_token = (user, password)

	# Trellis attributes
	sender = "local"
	seed_id = 123
	previous_event_id = 456

	# Make sure local instance of database is running
	driver = neo4j.GraphDatabase.driver(bolt_uri, auth=auth_token)

	def test_create_fastq_query(cls):

		query = main.create_parameterized_merge_query(cls.fastq_label, cls.fastq_query_parameters)

		# Run the query against a local database
		with cls.driver.session() as session:
			result = session.run(query, cls.fastq_query_parameters)
			graph = result.graph()
			result_summary = result.consume()

		nodes = list(graph.nodes)
		assert len(nodes) == 1
		node = nodes[0]
		assert len(node.labels) == 1
		assert frozenset({"Fastq"}) == node.labels
		#pdb.set_trace()

