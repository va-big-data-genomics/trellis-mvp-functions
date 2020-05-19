import json
import mock
import base64
import google
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

    def test_expected(self):
        data = {
            'header': {
                'method': 'VIEW', 
                'resource': 'queryResult', 
                'labels': ['Trigger', 'Import', 'BigQuery', 'Contamination', 'Cypher', 'Query', 'Database', 'Result'], 
                'sentFrom': 'wgs35-db-query', 
                'seedId': '1062325217821887', 
                'previousEventId': '1062332838591023'
            }, 
            'body': {
                'cypher': 'MATCH (s:CromwellStep)-[:OUTPUT]->(node:Blob) WHERE node.id ="***REMOVED***-dev-from-personalis-wgs35/***REMOVED***/***REMOVED***/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination/***REMOVED***.preBqsr.selfSM/1585064324765059" AND s.wdlCallAlias = "checkcontamination" AND NOT (node)-[:INPUT_TO]->(:JobRequest:BigQueryAppendTsv) CREATE (jr:JobRequest:BigQueryAppendTsv { sample: node.sample, nodeCreated: datetime(), nodeCreatedEpoch: datetime().epochSeconds, name: "bigquery-append-tsv", eventId: 1062332023587484 }) MERGE (node)-[:INPUT_TO]->(jr) RETURN node LIMIT 1', 
                'results': {'node': {'wdlCallAlias': 'CheckContamination', 'filetype': 'selfSM', 'extension': 'preBqsr.selfSM', 'plate': '***REMOVED***', 'trellisTaskId': '200323-224846-831-1d509436', 'dirname': '***REMOVED***/***REMOVED***/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination', 'path': '***REMOVED***/***REMOVED***/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination/***REMOVED***.preBqsr.selfSM', 'storageClass': 'REGIONAL', 'md5Hash': '+wodn8rzpnwpsS8cLlvHQQ==', 'timeCreatedEpoch': 1585064324.764, 'timeUpdatedEpoch': 1585064324.764, 'timeCreated': '2020-03-24T15:38:44.764Z', 'id': '***REMOVED***-dev-from-personalis-wgs35/***REMOVED***/***REMOVED***/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination/***REMOVED***.preBqsr.selfSM/1585064324765059', 'contentType': 'application/octet-stream', 'generation': '1585064324765059', 'nodeIteration': 'initial', 'metageneration': '1', 'kind': 'storage#object', 'timeUpdatedIso': '2020-03-24T15:38:44.764000+00:00', 'trellisTask': 'gatk-5-dollar', 'cromwellWorkflowName': 'germline_single_sample_workflow', 'sample': '***REMOVED***', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-wgs35/o/***REMOVED***%2F***REMOVED***%2Fgatk-5-dollar%2F200323-224846-831-1d509436%2Foutput%2Fgermline_single_sample_workflow%2F697594a9-165b-4f1e-9ee3-6e6a39cb6c88%2Fcall-CheckContamination%2F***REMOVED***.preBqsr.selfSM?generation=1585064324765059&alt=media', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-wgs35/o/***REMOVED***%2F***REMOVED***%2Fgatk-5-dollar%2F200323-224846-831-1d509436%2Foutput%2Fgermline_single_sample_workflow%2F697594a9-165b-4f1e-9ee3-6e6a39cb6c88%2Fcall-CheckContamination%2F***REMOVED***.preBqsr.selfSM', 'labels': ['WGS35', 'Blob', 'Cromwell', 'Gatk', 'Structured', 'Text', 'Data', 'CheckContamination'], 'nodeCreated': 1585064325422, 'bucket': '***REMOVED***-dev-from-personalis-wgs35', 'basename': '***REMOVED***.preBqsr.selfSM', 'crc32c': '5mVCSg==', 'size': 237, 'timeStorageClassUpdated': '2020-03-24T15:38:44.764Z', 'name': '***REMOVED***', 'etag': 'CIOrmOC4s+gCEAE=', 'timeCreatedIso': '2020-03-24T15:38:44.764000+00:00', 'cromwellWorkflowId': '697594a9-165b-4f1e-9ee3-6e6a39cb6c88', 'triggerOperation': 'finalize', 'updated': '2020-03-24T15:38:44.764Z'}}
            }
        }
        data_str = json.dumps(data)
        data_utf8 = data_str.encode('utf-8')
        event = {'data': base64.b64encode(data_utf8)}

        message = main.TrellisMessage(event, mock_context)

        # Check that everything asserts correctly
        assert message.event_id == mock_context.event_id
        assert message.seed_id  == data['header']['seedId']
        assert message.header   == data['header']
        assert message.body     == data['body']
        assert message.results  == data['body']['results']
        assert message.node     == data['body']['results']['node']

    def test_no_results(self):
        data = {
                 'header': {},
                 'body': {
                          'results': {}
                 }
        }
        data_str = json.dumps(data)
        data_utf8 = data_str.encode('utf-8')
        event = {'data': base64.b64encode(data_utf8)}

        message = main.TrellisMessage(event, mock_context)

        # Check that everything asserts correctly
        assert message.event_id == mock_context.event_id
        assert message.seed_id  == mock_context.event_id
        assert message.header   == data['header']
        assert message.body     == data['body']
        assert message.results  == data['body']['results']
        assert message.node     == None

    def test_no_seed(self):
        data = {
            'header': {
                'method': 'VIEW', 
                'resource': 'queryResult', 
                'labels': ['Trigger', 'Import', 'BigQuery', 'Contamination', 'Cypher', 'Query', 'Database', 'Result'], 
                'sentFrom': 'wgs35-db-query', 
                'previousEventId': '1062332838591023'
            }, 
            'body': {
                'cypher': 'MATCH (s:CromwellStep)-[:OUTPUT]->(node:Blob) WHERE node.id ="***REMOVED***-dev-from-personalis-wgs35/***REMOVED***/***REMOVED***/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination/***REMOVED***.preBqsr.selfSM/1585064324765059" AND s.wdlCallAlias = "checkcontamination" AND NOT (node)-[:INPUT_TO]->(:JobRequest:BigQueryAppendTsv) CREATE (jr:JobRequest:BigQueryAppendTsv { sample: node.sample, nodeCreated: datetime(), nodeCreatedEpoch: datetime().epochSeconds, name: "bigquery-append-tsv", eventId: 1062332023587484 }) MERGE (node)-[:INPUT_TO]->(jr) RETURN node LIMIT 1', 
                'results': {'node': {'wdlCallAlias': 'CheckContamination', 'filetype': 'selfSM', 'extension': 'preBqsr.selfSM', 'plate': '***REMOVED***', 'trellisTaskId': '200323-224846-831-1d509436', 'dirname': '***REMOVED***/***REMOVED***/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination', 'path': '***REMOVED***/***REMOVED***/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination/***REMOVED***.preBqsr.selfSM', 'storageClass': 'REGIONAL', 'md5Hash': '+wodn8rzpnwpsS8cLlvHQQ==', 'timeCreatedEpoch': 1585064324.764, 'timeUpdatedEpoch': 1585064324.764, 'timeCreated': '2020-03-24T15:38:44.764Z', 'id': '***REMOVED***-dev-from-personalis-wgs35/***REMOVED***/***REMOVED***/gatk-5-dollar/200323-224846-831-1d509436/output/germline_single_sample_workflow/697594a9-165b-4f1e-9ee3-6e6a39cb6c88/call-CheckContamination/***REMOVED***.preBqsr.selfSM/1585064324765059', 'contentType': 'application/octet-stream', 'generation': '1585064324765059', 'nodeIteration': 'initial', 'metageneration': '1', 'kind': 'storage#object', 'timeUpdatedIso': '2020-03-24T15:38:44.764000+00:00', 'trellisTask': 'gatk-5-dollar', 'cromwellWorkflowName': 'germline_single_sample_workflow', 'sample': '***REMOVED***', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-wgs35/o/***REMOVED***%2F***REMOVED***%2Fgatk-5-dollar%2F200323-224846-831-1d509436%2Foutput%2Fgermline_single_sample_workflow%2F697594a9-165b-4f1e-9ee3-6e6a39cb6c88%2Fcall-CheckContamination%2F***REMOVED***.preBqsr.selfSM?generation=1585064324765059&alt=media', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-wgs35/o/***REMOVED***%2F***REMOVED***%2Fgatk-5-dollar%2F200323-224846-831-1d509436%2Foutput%2Fgermline_single_sample_workflow%2F697594a9-165b-4f1e-9ee3-6e6a39cb6c88%2Fcall-CheckContamination%2F***REMOVED***.preBqsr.selfSM', 'labels': ['WGS35', 'Blob', 'Cromwell', 'Gatk', 'Structured', 'Text', 'Data', 'CheckContamination'], 'nodeCreated': 1585064325422, 'bucket': '***REMOVED***-dev-from-personalis-wgs35', 'basename': '***REMOVED***.preBqsr.selfSM', 'crc32c': '5mVCSg==', 'size': 237, 'timeStorageClassUpdated': '2020-03-24T15:38:44.764Z', 'name': '***REMOVED***', 'etag': 'CIOrmOC4s+gCEAE=', 'timeCreatedIso': '2020-03-24T15:38:44.764000+00:00', 'cromwellWorkflowId': '697594a9-165b-4f1e-9ee3-6e6a39cb6c88', 'triggerOperation': 'finalize', 'updated': '2020-03-24T15:38:44.764Z'}}
            }
        }
        data_str = json.dumps(data)
        data_utf8 = data_str.encode('utf-8')
        event = {'data': base64.b64encode(data_utf8)}

        message = main.TrellisMessage(event, mock_context)

                # Check that everything asserts correctly
        assert message.event_id == mock_context.event_id
        assert message.seed_id  == mock_context.event_id


class TestLoadJson:

    def test_expected(self):
        data = main.load_json('postgres-config.json')
        assert len(data.keys())        == 2
        assert len(data['CSV'].keys()) == 3
        assert len(data['preBqsr.selfSM'].keys()) == 1


class TestCheckConditions:

    def test_expected(self):
        data_labels = ['CheckContamination']
        node = {'labels': ['WGS35', 'Blob', 'Cromwell', 'Gatk', 'Structured', 'Text', 'Data', 'CheckContamination']}

        result = main.check_conditions(data_labels, node)
        assert result == True

    def test_empty_labels(self):
        data_labels = ['CheckContamination']
        node = {'labels': []}

        result = main.check_conditions(data_labels, node)
        assert result == False

    def test_missing_data_label(self):
        data_labels = ['CheckContamination']
        node = {'labels': ['WGS35', 'Blob', 'Cromwell', 'Gatk', 'Structured', 'Text', 'Data']}

        result = main.check_conditions(data_labels, node)
        assert result == False


class TestGetTableConfigData:

    def test_expected(self):
        table_configs = {"CheckContamination" : {}}
        node = {'labels': ['WGS35', 'Blob', 'Cromwell', 'Gatk', 'Structured', 'Text', 'Data', 'CheckContamination']}

        result = main.get_table_config_data(
                                            table_configs,
                                            node)
        assert result == table_configs['CheckContamination']

    def test_missing_label(self):
        table_configs = {"CheckContamination" : {}}
        node = {'labels': ['WGS35', 'Blob', 'Cromwell', 'Gatk', 'Structured', 'Text', 'Data']}
        
        with pytest.raises(KeyError):
            main.get_table_config_data(
                                       table_configs,
                                       node)