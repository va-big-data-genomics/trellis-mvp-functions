#!/usr/bin/env python3

import pdb
import json
import mock
import yaml
import neo4j
import base64
import pytest

from unittest import TestCase

#import trellisdata as trellis
#from trellisdata import DatabaseQuery
import main

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

fastq_to_ubam_body = {
	'nodes': [], 
	'queryName': 'launchFastqToUbam', 
	'relationship': {
		'end_node': {
	  		'id': 650052, 
	  		'labels': ['Fastq'], 
	  		'properties': {
	  			'basename': 'SHIP123_3_R2.fastq.gz', 
	  			'bucket': 'va-big-data-genomics-project', 
	  			'componentCount': 1, 
	  			'crc32c': 'RLajXg==', 
	  			'dirname': 'va_mvp_phase2/DVALABP123/SHIP123/FASTQ', 
	  			'etag': 'CNGA7Lv33/YCEAQ=', 
	  			'extension': 'fastq.gz', 
	  			'filetype': 'gz', 
	  			'generation': '1648165639618641', 
	  			'gitCommitHash': '80423e1', 
	  			'gitVersionTag': '', 
	  			'id': 'va-big-data-genomics-project/va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R2.fastq.gz/1648165639618641', 
	  			'kind': 'storage#object', 
	  			'matePair': 2, 
	  			'mediaLink': 'https://storage.googleapis.com/download/storage/v1/b/va-big-data-genomics-project/o/va_mvp_phase2%2FDVALABP123%2FSHIP123%2FFASTQ%2FSHIP123_3_R2.fastq.gz?generation=1648165639618641&alt=media', 
	  			'metadata': "{'pbilling': '2022-11-28-try0', 'trellis-uuid': '0d044882-733e-42d7-bcce-a19c80815d3b'}", 
	  			'metageneration': '4', 
	  			'name': 'SHIP123_3_R2', 
	  			'nodeCreated': 1669686815117, 
	  			'nodeIteration': 'initial', 
	  			'path': 'va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R2.fastq.gz', 
	  			'plate': 'DVALABP123', 
	  			'readGroup': 3, 
	  			'sample': 'SHIP123', 
	  			'selfLink': 'https://www.googleapis.com/storage/v1/b/va-big-data-genomics-project/o/va_mvp_phase2%2FDVALABP123%2FSHIP123%2FFASTQ%2FSHIP123_3_R2.fastq.gz', 
	  			'size': 6427873172, 
	  			'storageClass': 'REGIONAL', 
	  			'timeCreated': '2022-03-24T23:47:19.743Z', 
	  			'timeCreatedEpoch': 1648165639.743, 
	  			'timeCreatedIso': '2022-03-24T23:47:19.743000+00:00', 
	  			'timeStorageClassUpdated': '2022-03-24T23:47:19.743Z', 
	  			'timeUpdatedEpoch': 1669686812.647, 
	  			'timeUpdatedIso': '2022-11-29T01:53:32.647000+00:00', 
	  			'trellisUuid': '0d044882-733e-42d7-bcce-a19c80815d3b', 
	  			'triggerOperation': 'metadataUpdate', 
	  			'updated': '2022-11-29T01:53:32.647Z', 
	  			'uri': 'gs://va-big-data-genomics-project/va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R2.fastq.gz'
	  		}
		}, 
		'id': 286403, 
		'properties': {}, 
		'start_node': {
			'id': 650051, 
			'labels': ['Fastq'], 
			'properties': {
				'basename': 'SHIP123_3_R1.fastq.gz', 
				'bucket': 'va-big-data-genomics-project', 
				'componentCount': 1, 
				'crc32c': '51pfIQ==', 
				'dirname': 'va_mvp_phase2/DVALABP123/SHIP123/FASTQ', 
				'etag': 'CNOQ+pX33/YCEAQ=', 
				'extension': 'fastq.gz', 
				'filetype': 'gz', 
				'generation': '1648165560158291', 
				'gitCommitHash': '80423e1', 
				'gitVersionTag': '', 
				'id': 'va-big-data-genomics-project/va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R1.fastq.gz/1648165560158291', 
				'kind': 'storage#object', 
				'matePair': 1, 
				'mediaLink': 'https://storage.googleapis.com/download/storage/v1/b/va-big-data-genomics-project/o/va_mvp_phase2%2FDVALABP123%2FSHIP123%2FFASTQ%2FSHIP123_3_R1.fastq.gz?generation=1648165560158291&alt=media', 
				'metadata': "{'pbilling': '2022-11-28-try0', 'trellis-uuid': 'ab8286eb-a17d-422b-9ae4-52039e866cb8'}", 
				'metageneration': '4', 
				'name': 'SHIP123_3_R1', 
				'nodeCreated': 1669686805613, 
				'nodeIteration': 'initial', 
				'path': 'va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R1.fastq.gz', 
				'plate': 'DVALABP123', 
				'readGroup': 3, 
				'sample': 'SHIP123', 
				'selfLink': 'https://www.googleapis.com/storage/v1/b/va-big-data-genomics-project/o/va_mvp_phase2%2FDVALABP123%2FSHIP123%2FFASTQ%2FSHIP123_3_R1.fastq.gz', 
				'size': 6204274820, 
				'storageClass': 'REGIONAL', 
				'timeCreated': '2022-03-24T23:46:00.369Z', 
				'timeCreatedEpoch': 1648165560.369, 
				'timeCreatedIso': '2022-03-24T23:46:00.369000+00:00', 
				'timeStorageClassUpdated': '2022-03-24T23:46:00.369Z', 
				'timeUpdatedEpoch': 1669686801.582, 
				'timeUpdatedIso': '2022-11-29T01:53:21.582000+00:00', 
				'trellisUuid': 'ab8286eb-a17d-422b-9ae4-52039e866cb8', 
				'triggerOperation': 'metadataUpdate', 
				'updated': '2022-11-29T01:53:21.582Z', 
				'uri': 'gs://va-big-data-genomics-project/va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R1.fastq.gz'
			}
		}, 
		'type': 'HAS_MATE_PAIR'
	}, 
	'resultSummary': {
		'counters': {
			'labels_added': 1, 
			'nodes_created': 1, 
			'properties_set': 4, 
			'relationships_created': 2
		}, 
		'database': None, 
		'notifications': None, 
		'parameters': {
			'read_group': 3, 
			'sample': 'SHIP123'
		}, 
		'plan': None, 
		'profile': None, 
		'query': "MATCH (seq:PersonalisSequencing)-[:GENERATED]->(r1:Fastq)-[rel:HAS_MATE_PAIR]->(r2:Fastq) WHERE seq.sample = $sample AND r1.readGroup = $read_group AND r2.readGroup = $read_group AND r1.matePair = 1 AND r2.matePair = 2 AND NOT (r1)-[:WAS_USED_BY]->(:JobRequest {name: 'fastq-to-ubam'}) AND NOT (r2)-[:WAS_USED_BY]->(:JobRequest {name: 'fastq-to-ubam'}) CREATE (job_request:JobRequest {name: 'fastq-to-ubam', sample: $sample, nodeCreated: datetime(), nodeCreatedEpoch: datetime().epochSeconds}) WITH r1, rel, r2, job_request LIMIT 1 MERGE (r1)-[:WAS_USED_BY]->(job_request) MERGE (r2)-[:WAS_USED_BY]->(job_request) RETURN r1, rel, r2", 
		'query_type': 'rw', 
		'result_available_after': 7, 
		'result_consumed_after': 1
	}
}

class JobLauncher(yaml.YAMLObject):
	yaml_tag = u'!JobLauncher'
	def __init__(
			     self, 
				 name,
				 inputs,
				 virtual_machine,
				 dsub):
		self.name = name
		self.inputs = inputs
		self.virtual_machine = virtual_machine
		self.dsub = dsub


class TestLauncherConfiguration(TestCase):

	def test_load_launcher_document(cls):
		tasks = {}
		launcher_doc = 'job-launcher-pilot.yaml'
		with open(launcher_doc, 'r') as file_handle:
			task_generator = yaml.load_all(
								file_handle, 
								Loader=yaml.FullLoader)
			for task in task_generator:
				tasks[task.name] = task

		task = tasks['fastq-to-ubam']
		assert task.dsub['command']


class TestGetJobValues(TestCase):
	tasks = {}
	launcher_doc = 'job-launcher-pilot.yaml'
	with open(launcher_doc, 'r') as file_handle:
		task_generator = yaml.load_all(
							file_handle, 
							Loader=yaml.FullLoader)
		for task in task_generator:
			tasks[task.name] = task

	task = tasks['fastq-to-ubam']
	start = fastq_to_ubam_body['relationship']['start_node']
	end = fastq_to_ubam_body['relationship']['end_node']
	
	def test_fq2u_get_env_variables(cls):
		values = main._get_job_values(
									  cls.task, 
									  cls.start, 
									  cls.end, 
									  "env_variables")
		assert len(values.keys()) == 3
		assert values['RG'] == cls.start['properties']['readGroup']
		assert values['SM'] == cls.start['properties']['sample']
		assert values['PL'] == 'illumina'

	def test_fq2u_get_inputs(cls):
		values = main._get_job_values(
									  cls.task,
									  cls.start,
									  cls.end,
									  "inputs")
		assert len(values.keys()) == 2
		assert values['FASTQ_R1'] == cls.start['properties']['uri']
		assert values['FASTQ_R2'] == cls.end['properties']['uri']


class TestGetOutputValues(TestCase):
	tasks = {}
	launcher_doc = 'job-launcher-pilot.yaml'
	with open(launcher_doc, 'r') as file_handle:
		task_generator = yaml.load_all(
							file_handle, 
							Loader=yaml.FullLoader)
		for task in task_generator:
			tasks[task.name] = task

	task = tasks['fastq-to-ubam']
	start = fastq_to_ubam_body['relationship']['start_node']
	end = fastq_to_ubam_body['relationship']['end_node']

	def test_fq2u_outputs(cls):
		values = main._get_output_values(
									  	 task = cls.task,
									  	 bucket = "out-bucket",
									  	 start = cls.start,
									  	 end = cls.end,
									     job_id = "job-id-123")
		assert len(values.keys()) == 1
		assert values['UBAM'] == "gs://out-bucket/fastq-to-ubam/job-id-123/output/DVALABP123/SHIP123/SHIP123_3.ubam"


class TestGetLabelValues(TestCase):
	tasks = {}
	launcher_doc = 'job-launcher-pilot.yaml'
	with open(launcher_doc, 'r') as file_handle:
		task_generator = yaml.load_all(
							file_handle, 
							Loader=yaml.FullLoader)
		for task in task_generator:
			tasks[task.name] = task

	task = tasks['fastq-to-ubam']
	start = fastq_to_ubam_body['relationship']['start_node']
	end = fastq_to_ubam_body['relationship']['end_node']

	def test_fq2u_outputs(cls):
		values = main._get_label_values(
									  	cls.task,
									  	cls.start,
									  	cls.end)
		assert len(values.keys()) == 3
		assert values['read-group'] == str(cls.start['properties']['readGroup']).lower()
		assert values['sample'] == str(cls.start['properties']['sample']).lower()
		assert values['plate'] == str(cls.start['properties']['plate']).lower()


class TestCreateJobDict(TestCase):

	start = fastq_to_ubam_body['relationship']['start_node']
	end = fastq_to_ubam_body['relationship']['end_node']
	project_id = 'va-big-data-genomics-project'

	trellis_config = {
		'DSUB_USER': 'trellis',
		'DSUB_REGIONS': 'us-west1',
		'NETWORK': 'trellis',
		'SUBNETWORK': 'trellis-subnet',
		'DSUB_OUT_BUCKET': 'out-bucket',
		'DSUB_LOG_BUCKET': 'log-bucket'
	}

	tasks = {}
	launcher_doc = 'job-launcher-pilot.yaml'
	with open(launcher_doc, 'r') as file_handle:
		task_generator = yaml.load_all(
							file_handle, 
							Loader=yaml.FullLoader)
		for task in task_generator:
			tasks[task.name] = task
	task = tasks['fastq-to-ubam']

	def test_create_fastq_to_ubam_inputs(cls):
		start = fastq_to_ubam_body['relationship']['start_node']
		end = fastq_to_ubam_body['relationship']['end_node']

		tasks = {}
		launcher_doc = 'job-launcher-pilot.yaml'
		with open(launcher_doc, 'r') as file_handle:
			task_generator = yaml.load_all(
								file_handle, 
								Loader=yaml.FullLoader)
			for task in task_generator:
				tasks[task.name] = task
		task = tasks['fastq-to-ubam']

		inputs = main._get_job_values(
			                 		  task = task,
			                  		  start = start,
			                 		  end = end,
			                 		  params = "inputs")
		assert inputs['FASTQ_R1'] == start['properties']['uri']
		assert inputs['FASTQ_R2'] == end['properties']['uri']

	def test_create_fastq_to_ubam_env_vars(cls):
		env_vars = main._get_job_values(
										task = cls.task,
										start = cls.start,
										end = cls.end,
										params = 'env_variables')
		assert env_vars == {'RG': 3, 'SM': 'SHIP123', 'PL': 'illumina'}

	def test_create_fastq_to_ubam_outputs(cls):
		outputs = main._get_output_values(
										  task = cls.task,
										  bucket = 'output-bucket',
										  start = cls.start,
										  end = cls.end,
										  job_id = 'job123')
		assert outputs == {'UBAM': 'gs://output-bucket/fastq-to-ubam/job123/output/DVALABP123/SHIP123/SHIP123_3.ubam'}

	def test_create_fastq_to_ubam_labels(cls):
		labels = main._get_label_values(
										task = cls.task,
										start = cls.start,
										end = cls.end)
		assert labels == {'read-group': '3', 'sample': 'ship123', 'plate': 'dvalabp123'}

	def test_create_fastq_to_ubam_job_dict(cls):
		input_ids = []
		input_ids.append(cls.start['properties']['id'])
		input_ids.append(cls.end['properties']['id'])


		job_dict = main.create_job_dict(
										task = cls.task,
										project_id = cls.project_id,
										trellis_config = cls.trellis_config,
										start_node = cls.start,
										end_node = cls.end,
										job_id = 'job123',
										input_ids = input_ids,
										trunc_nodes_hash = '5d709493')
		assert job_dict == {'name': 'fastq-to-ubam', 'dsubName': 'fq2u-5d709', 'inputHash': '5d709493', 'inputIds': ['va-big-data-genomics-project/va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R1.fastq.gz/1648165560158291', 'va-big-data-genomics-project/va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R2.fastq.gz/1648165639618641'], 'trellisTaskId': 'job123', 'provider': 'google-v2', 'user': 'trellis', 'regions': 'us-west1', 'project': 'va-big-data-genomics-project', 'network': 'trellis', 'subnetwork': 'trellis-subnet', 'minCores': 1, 'minRam': 7.5, 'bootDiskSize': 20, 'image': 'gcr.io/va-big-data-genomics-project/broadinstitute/gatk:4.1.0.0', 'logging': 'gs://log-bucket/fastq-to-ubam/job123/logs', 'diskSize': 500, 'preemptible': False, 'command': "/gatk/gatk --java-options \\'-Xmx8G -Djava.io.tmpdir=bla\\' FastqToSam -F1 ${FASTQ_R1} -F2 ${FASTQ_R2} -O ${UBAM} -RG ${RG} -SM ${SM} -PL ${PL}", 'envs': {'RG': 3, 'SM': 'SHIP123', 'PL': 'illumina'}, 'inputs': {'FASTQ_R1': 'gs://va-big-data-genomics-project/va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R1.fastq.gz', 'FASTQ_R2': 'gs://va-big-data-genomics-project/va_mvp_phase2/DVALABP123/SHIP123/FASTQ/SHIP123_3_R2.fastq.gz'}, 'outputs': {'UBAM': 'gs://out-bucket/fastq-to-ubam/job123/output/DVALABP123/SHIP123/SHIP123_3.ubam'}, 'dsubLabels': {'read-group': '3', 'sample': 'ship123', 'plate': 'dvalabp123'}}



	