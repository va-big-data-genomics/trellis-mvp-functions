import mock
import base64
from uuid import uuid4

import pytest

import main

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'


class TestTrellisMessage:
    """Test the TrellisMessage class.
    
        Cases:
            - expected input
            - no node
            - no seed
    """

    def test_expected():
    event = {
        'header': {
            'method': 'VIEW', 
            'resource': 'queryResult', 
            'labels': ['Trigger', 'Import', 'BigQuery', 'Contamination', 'Cypher', 'Query', 'Database', 'Result'], 
            'sentFrom': 'wgs35-db-query', 
            'seedId': '1062325217821887', 
            'previousEventId': '1062332838591023'
        }, 
        'body': {
            'cypher': 'MATCH (s:CromwellStep)-[:OUTPUT]->(node:Blob) WHERE node.id ="gbsc-gcp-project-mvp-dev-from-personalis-wgs35/DVALABP000398/SHIP4946371/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination/SHIP4946371.preBqsr.selfSM/1585064324765059" AND s.wdlCallAlias = "checkcontamination" AND NOT (node)-[:INPUT_TO]->(:JobRequest:BigQueryAppendTsv) CREATE (jr:JobRequest:BigQueryAppendTsv { sample: node.sample, nodeCreated: datetime(), nodeCreatedEpoch: datetime().epochSeconds, name: "bigquery-append-tsv", eventId: 1062332023587484 }) MERGE (node)-[:INPUT_TO]->(jr) RETURN node LIMIT 1', 
            'results': {'node': {'wdlCallAlias': 'CheckContamination', 'filetype': 'selfSM', 'extension': 'preBqsr.selfSM', 'plate': 'DVALABP000398', 'trellisTaskId': '200323-224846-831-1d509436', 'dirname': 'DVALABP000398/SHIP4946371/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination', 'path': 'DVALABP000398/SHIP4946371/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination/SHIP4946371.preBqsr.selfSM', 'storageClass': 'REGIONAL', 'md5Hash': '+wodn8rzpnwpsS8cLlvHQQ==', 'timeCreatedEpoch': 1585064324.764, 'timeUpdatedEpoch': 1585064324.764, 'timeCreated': '2020-03-24T15:38:44.764Z', 'id': 'gbsc-gcp-project-mvp-dev-from-personalis-wgs35/DVALABP000398/SHIP4946371/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination/SHIP4946371.preBqsr.selfSM/1585064324765059', 'contentType': 'application/octet-stream', 'generation': '1585064324765059', 'nodeIteration': 'initial', 'metageneration': '1', 'kind': 'storage#object', 'timeUpdatedIso': '2020-03-24T15:38:44.764000+00:00', 'trellisTask': 'gatk-5-dollar', 'cromwellWorkflowName': 'germline_single_sample_workflow', 'sample': 'SHIP4946371', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-wgs35/o/DVALABP000398%2FSHIP4946371%2Fgatk-5-dollar%2F200323-224846-831-1d509436%2Foutput%2Fgermline_single_sample_workflow%2F697594a9-165b-4f1e-9ee3-6e6a39cb6c88%2Fcall-CheckContamination%2FSHIP4946371.preBqsr.selfSM?generation=1585064324765059&alt=media', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-dev-from-personalis-wgs35/o/DVALABP000398%2FSHIP4946371%2Fgatk-5-dollar%2F200323-224846-831-1d509436%2Foutput%2Fgermline_single_sample_workflow%2F697594a9-165b-4f1e-9ee3-6e6a39cb6c88%2Fcall-CheckContamination%2FSHIP4946371.preBqsr.selfSM', 'labels': ['WGS35', 'Blob', 'Cromwell', 'Gatk', 'Structured', 'Text', 'Data', 'CheckContamination'], 'nodeCreated': 1585064325422, 'bucket': 'gbsc-gcp-project-mvp-dev-from-personalis-wgs35', 'basename': 'SHIP4946371.preBqsr.selfSM', 'crc32c': '5mVCSg==', 'size': 237, 'timeStorageClassUpdated': '2020-03-24T15:38:44.764Z', 'name': 'SHIP4946371', 'etag': 'CIOrmOC4s+gCEAE=', 'timeCreatedIso': '2020-03-24T15:38:44.764000+00:00', 'cromwellWorkflowId': '697594a9-165b-4f1e-9ee3-6e6a39cb6c88', 'triggerOperation': 'finalize', 'updated': '2020-03-24T15:38:44.764Z'}}
        }
    }

    def test_no_results():
        event = {
                 'header': {},
                 'body': {
                          'results': {}
                 }
        }
        message = main.TrellisMessage(event, mock_context)

    def test_no_seed():


class TestLoadJson:


class TestCheckConditions:


class TestGetBigQueryConfigData:


class TestMakeGcsUri:


class TestConfigLoadJob:


class TestMakeTableSchema:
    
# Test Trellis message
def test_trellis_message_none():
    body = {'results': {}}
    message = main.TrellisMessage(event, context)