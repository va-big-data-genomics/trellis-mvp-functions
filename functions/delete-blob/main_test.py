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
                         'name': 'projects/gbsc-gcp-project-mvp-dev/topics/delete-blob', 
                         'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage'
}

CLIENT = storage.Client()

class TestDeleteBlob:

    def test_expected(self):
        result = main.delete_blob(
                                  client = CLIENT,
                                  bucket = 'gbsc-gcp-project-mvp-dev-trellis',
                                  path = 'README.txt',
                                  dry_run = True)
        assert result == True


class TestCheckProtectedPatterns:

    def test_bam(self):
        path = "DVALABP000398/SHIP4946376/gatk-5-dollar/201016-005839-926-ef44df7f/output/germline_single_sample_workflow/6fd6a37d-a1e5-4d8d-b415-9b3d0682583f/call-MarkDuplicates/SHIP4946376.aligned.unsorted.duplicates_marked.bam"

        result = main.check_protected_patterns(path)
        assert result == False

    def test_vcf(self):
        path = "DVALABP000398/SHIP4946372/gatk-5-dollar/201016-010142-962-a6fc4c8a/output/germline_single_sample_workflow/3b4eda76-6fa3-4f84-bce2-ce76825c7edc/call-HaplotypeCaller4/shard-46/SHIP4946372.vcf.gz"

        result = main.check_protected_patterns(path)
        assert result == False

    def test_cram(self):
        path = "DVALABP000398/SHIP4946371/gatk-5-dollar/201016-005855-366-0ee9d9a6/output/germline_single_sample_workflow/3fb18586-5ded-4967-8303-30c0bba2cda7/call-ConvertToCram/SHIP4946371.cram"

        result = main.check_protected_patterns(path)
        assert result == True

    def test_crai(self):
        path = "DVALABP000398/SHIP4946371/gatk-5-dollar/201016-005855-366-0ee9d9a6/output/germline_single_sample_workflow/3fb18586-5ded-4967-8303-30c0bba2cda7/call-ConvertToCram/SHIP4946371.cram.crai"

        result = main.check_protected_patterns(path)
        assert result == True

    def test_fastq(self):
        path = "va_mvp_phase2/DVALABP000398/SHIP4946371/FASTQ/SHIP4946371_1_R1.fastq.gz"

        result = main.check_protected_patterns(path)
        assert result == True

    def test_gvcf(self):
        path = "DVALABP000398/SHIP4946371/gatk-5-dollar/201016-005855-366-0ee9d9a6/output/germline_single_sample_workflow/3fb18586-5ded-4967-8303-30c0bba2cda7/call-MergeVCFs/SHIP4946371.g.vcf.gz"

        result = main.check_protected_patterns(path)
        assert result == True

    def test_gvcf_tbi(self):
        path = "DVALABP000398/SHIP4946371/gatk-5-dollar/201016-005855-366-0ee9d9a6/output/germline_single_sample_workflow/3fb18586-5ded-4967-8303-30c0bba2cda7/call-MergeVCFs/SHIP4946371.g.vcf.gz.tbi"
        
        result = main.check_protected_patterns(path)
        assert result == True

    def test_flagstat(self):
        path = "DVALABP000398/SHIP4946371/flagstat/201016-164150-761-9c958716/output/SHIP4946371.bam.flagstat.data.tsv"

        result = main.check_protected_patterns(path)
        assert result == True

    def test_fastqs(self):
        path = "DVALABP000398/SHIP4946371/bam-fastqc/201016-164150-310-9c958716/output/SHIP4946371.bam.fastqc.data.txt"
        
        result = main.check_protected_patterns(path)
        assert result == True

    def test_vcfstats(self):
        path = "DVALABP000398/SHIP4946371/vcfstats/201016-211551-384-09f32fd4/output/SHIP4946371.rtg.vcfstats.data.txt"

        result = main.check_protected_patterns(path)
        assert result == True


class TestMain:

    def test_expected(self):
        data = {
                'body': {
                         'cypher': 'MATCH (s:PersonalisSequencing)-[:GENERATED|WAS_USED_BY|LED_TO*]->(b:Blob) WHERE s.sample = "SHIP4946369" WITH COLLECT(DISTINCT(b)) AS all_blobs UNWIND all_blobs AS b MATCH p=(b)-[*1..2]-(:BiologicalOme) WHERE ALL (r in relationships(p) WHERE r.ontology="bioinformatics") WITH all_blobs, COLLECT(b) AS essential_blobs UNWIND all_blobs AS b MATCH (b) WHERE NOT b IN essential_blobs AND (NOT b.obj_exists = false OR NOT EXISTS(b.obj_exists)) AND b.bucket = "gbsc-gcp-project-mvp-dev-from-personalis-phase3-data" RETURN b.bucket AS bucket, b.path AS path ORDER BY b.size DESC LIMIT 100', 
                         'results': [
                                     {
                                      'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis-phase3-data', 
                                      'path': 'DVALABP000398/SHIP4946369/gatk-5-dollar/201016-010047-632-ed170a4a/output/germline_single_sample_workflow/392d181f-d459-4ae9-bda4-3ef528f67286/call-MarkDuplicates/SHIP4946369.aligned.unsorted.duplicates_marked.bam'
                                     }, 
                                     {
                                      'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis-phase3-data', 
                                      'path': 'DVALABP000398/SHIP4946369/gatk-5-dollar/201016-010047-632-ed170a4a/output/germline_single_sample_workflow/392d181f-d459-4ae9-bda4-3ef528f67286/call-SortSampleBam/SHIP4946369.aligned.duplicate_marked.sorted.bam'
                                     }
                                    ]
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