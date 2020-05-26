import json
import mock
import base64
import google
from uuid import uuid4

from psycopg2.extensions import Column

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
        assert len(data['SELFSM'].keys()) == 1


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


class TestCheckTableExists:

    def test_table_does_not_exist(self):
        table_name = "check_contamination"
        fetchone_result = (False,)

        with mock.patch('psycopg2.connect') as mock_connect:
            mock_connect.cursor.return_value.fetchone.return_value = fetchone_result
            table_exists = main.check_table_exists(mock_connect, table_name)

            assert table_exists == False

    def test_table_does_exist(self):
        table_name = "check_contamination"
        fetchone_result = (True,)

        with mock.patch('psycopg2.connect') as mock_connect:
            mock_connect.cursor.return_value.fetchone.return_value = fetchone_result
            table_exists = main.check_table_exists(mock_connect, table_name)

            assert table_exists == True


class TestGetDelimiter:

    def test_check_contamination(self):
        node = {
                "extension": "preBqsr.selfSM",
                "filetype": "selfSM"
        }
        delimiter = main.get_delimiter(node)

        assert delimiter == "\t"

    def test_csv(self):
        node = {
                "extension": "csv",
                "filetype": "csv"
        }
        delimiter = main.get_delimiter(node)

        assert delimiter == ","

    def test_no_rule(self):
        node = {
                "extension": "nonsense_extension",
                "filetype": "nonsense_extension"
        }
        delimiter = main.get_delimiter(node)

        assert delimiter == None

"""
class TestGetTableColumnNames:

    def test_check_contamination_columns(self):
        table_name = "check_contamination"
        description =(Column(name='seq_id', type_code=1043), Column(name='rg', type_code=1043), Column(name='chip_id', type_code=1043), Column(name='snps', type_code=1043), Column(name='reads', type_code=1043), Column(name='avg_dp', type_code=1043), Column(name='freemix', type_code=1043), Column(name='freelk1', type_code=1043), Column(name='freelk0', type_code=1043), Column(name='free_rh', type_code=1043), Column(name='free_ra', type_code=1043), Column(name='chipmix', type_code=1043), Column(name='chiplk1', type_code=1043), Column(name='chiplk0', type_code=1043), Column(name='chip_rh', type_code=1043), Column(name='chip_ra', type_code=1043), Column(name='dpref', type_code=1043), Column(name='rdphet', type_code=1043), Column(name='rdpalt', type_code=1043))

        with open('postgres-config.json') as fh:
            data = json.load(fh)
        schema_fields = data["PREBQSR.SELFSM"]["CheckContamination"]["schema-fields"]

        with mock.patch('psycopg2.connect') as mock_connect:
            mock_connect.cursor.return_value.description.return_value = description
            col_names = main.get_table_col_names(mock_connect, table_name)

            schema_keys = schema_fields.keys()
            keys = [key.lower() for key in schema_keys]
            assert col_names == keys
"""