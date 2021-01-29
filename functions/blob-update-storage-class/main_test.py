import json
import mock
import base64
import google
from uuid import uuid4
import pytest

from google.cloud import storage

import main

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'
mock_context.resource = {
                         'service': 'pubsub.googleapis.com', 
                         'name': 'projects/my-gcp-project-dev/topics/blob-update-storage-class', 
                         'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage'
}

CLIENT = storage.Client()

class TestUpdateStorageClass:

    def test_expected(self):
        result = main.update_storage_class(
                                  client = CLIENT,
                                  bucket = 'my-gcp-project-dev-trellis',
                                  path = 'README.txt',
                                  storage_class = 'COLDLINE',
                                  dry_run = True)
        assert result == 'COLDLINE'


class TestCheckStorageClassRequest:

    def test_bam(self):
        # Neither extension nor requested class are supported (1)
        extension = "aligned.unsorted.duplicates_marked.bam"
        current_class = "REGIONAL"
        requested_class = "COLDLINE"

        result = main.check_storage_class_request(extension, current_class, requested_class)
        assert result == False

    def test_vcf(self):
        # Neither extension nor requested class are supported (2)
        extension = "vcf.gz"
        current_class = "REGIONAL"
        requested_class = "ARCHIVE"

        result = main.check_storage_class_request(extension, current_class, requested_class)
        assert result == False

    def test_fastq(self):
        # Extension & requested class are supported
        extension = "fastq.gz"
        current_class = "REGIONAL"
        requested_class = "COLDLINE"

        result = main.check_storage_class_request(extension, current_class, requested_class)
        assert result == "COLDLINE_STORAGE_CLASS"

    def test_fastq(self):
        # Extension is supported but not requested class
        extension = "fastq.gz"
        current_class = "REGIONAL"
        requested_class = "ARCHIVE"

        result = main.check_storage_class_request(extension, current_class, requested_class)
        assert result == False


class TestMain:

    def test_expected(self):
        data = {
                'body': {
                         'cypher': 'MATCH (s:PersonalisSequencing)-[:GENERATED|WAS_USED_BY|LED_TO*]->(b:Blob) WHERE s.sample = "SHIP0" WITH COLLECT(DISTINCT(b)) AS all_blobs UNWIND all_blobs AS b MATCH p=(b)-[*1..2]-(:BiologicalOme) WHERE ALL (r in relationships(p) WHERE r.ontology="bioinformatics") WITH all_blobs, COLLECT(b) AS essential_blobs UNWIND all_blobs AS b MATCH (b) WHERE NOT b IN essential_blobs AND (NOT b.obj_exists = false OR NOT EXISTS(b.obj_exists)) AND b.bucket = "my-gcp-project-dev-from-personalis-phase3-data" RETURN b.bucket AS bucket, b.path AS path ORDER BY b.size DESC LIMIT 100', 
                         'results': {
                                     'bucket': 'my-gcp-project-test-from-personalis', 
                                     'path': 'va_mvp_phase2/DVALABP0/SHIP0/FASTQ/SHIP0_3_R2.fastq.gz',
                                     'extension': 'fastq.gz',
                                     'current_class': 'REGIONAL',
                                     'requested_class': 'COLDLINE'
                         }, 
                },
                'header': {
                           'resource': 'queryResult',
                           'method': 'VIEW',
                           'dry-run': 'True'
                }
        }
        data_str = json.dumps(data)
        data_utf8 = data_str.encode('utf-8')
        event = {'data': base64.b64encode(data_utf8)}

        result = main.main(event, mock_context)
        assert result == 2