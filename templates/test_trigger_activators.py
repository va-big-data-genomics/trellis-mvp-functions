#!/usr/bin/env python3

import json
import mock
import base64
import pytest

import trellis_classes as trellis
import check_triggers

mock_context = mock.Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

with open("database-triggers.json", 'r') as fh:
    triggers_config = json.load(fh)

ALL_TRIGGERS = {}
for config in triggers_config:
    config["function_name"] = None
    config["env_vars"] = None

    trigger = trellis.DatabaseTrigger(**config)
    ALL_TRIGGERS[trigger.name] = trigger

class HaplotypeCallerTbi:

	def __init__(self):
		data = {'body': {'cypher': 'MATCH (step:CromwellStep { cromwellWorkflowId: "1b47bab1-eecd-4727-9589-b39b7d33d602", wdlCallAlias: "haplotypecaller4" }), (node:Blob { cromwellWorkflowId:"1b47bab1-eecd-4727-9589-b39b7d33d602", wdlCallAlias: "HaplotypeCaller4", id: "gbsc-gcp-project-mvp-from-personalis-phase3-data/DVALABP001859/SHIP6284138/gatk-5-dollar/211027-212441-674-eb993f50/output/germline_single_sample_workflow/1b47bab1-eecd-4727-9589-b39b7d33d602/call-HaplotypeCaller4/shard-32/SHIP6284138.vcf.gz.tbi/1635447289772377" }) MERGE (step)-[:GENERATED]->(node) RETURN node', 'results': {'node': {'basename': 'SHIP6284138.vcf.gz.tbi', 'bucket': 'gbsc-gcp-project-mvp-from-personalis-phase3-data', 'contentType': 'application/octet-stream', 'crc32c': 'EJnHYw==', 'cromwellWorkflowId': '1b47bab1-eecd-4727-9589-b39b7d33d602', 'cromwellWorkflowName': 'germline_single_sample_workflow', 'dirname': 'DVALABP001859/SHIP6284138/gatk-5-dollar/211027-212441-674-eb993f50/output/germline_single_sample_workflow/1b47bab1-eecd-4727-9589-b39b7d33d602/call-HaplotypeCaller4/shard-32', 'etag': 'CNnyhfbj7fMCEAE=', 'extension': 'vcf.gz.tbi', 'filetype': 'tbi', 'generation': '1635447289772377', 'gitCommitHash': '438a525', 'gitVersionTag': '', 'id': 'gbsc-gcp-project-mvp-from-personalis-phase3-data/DVALABP001859/SHIP6284138/gatk-5-dollar/211027-212441-674-eb993f50/output/germline_single_sample_workflow/1b47bab1-eecd-4727-9589-b39b7d33d602/call-HaplotypeCaller4/shard-32/SHIP6284138.vcf.gz.tbi/1635447289772377', 'kind': 'storage#object', 'labels': ['WGS35', 'Blob', 'Cromwell', 'Gatk', 'Tbi', 'Shard', 'Index'], 'md5Hash': 'ArsYnSABoQsitR1zA6ONUQ==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/gbsc-gcp-project-mvp-from-personalis-phase3-data/o/DVALABP001859%2FSHIP6284138%2Fgatk-5-dollar%2F211027-212441-674-eb993f50%2Foutput%2Fgermline_single_sample_workflow%2F1b47bab1-eecd-4727-9589-b39b7d33d602%2Fcall-HaplotypeCaller4%2Fshard-32%2FSHIP6284138.vcf.gz.tbi?generation=1635447289772377&alt=media', 'metageneration': '1', 'name': 'SHIP6284138', 'nodeCreated': 1635447290134, 'nodeIteration': 'initial', 'path': 'DVALABP001859/SHIP6284138/gatk-5-dollar/211027-212441-674-eb993f50/output/germline_single_sample_workflow/1b47bab1-eecd-4727-9589-b39b7d33d602/call-HaplotypeCaller4/shard-32/SHIP6284138.vcf.gz.tbi', 'plate': 'DVALABP001859', 'sample': 'SHIP6284138', 'selfLink': 'https://www.googleapis.com/storage/v1/b/gbsc-gcp-project-mvp-from-personalis-phase3-data/o/DVALABP001859%2FSHIP6284138%2Fgatk-5-dollar%2F211027-212441-674-eb993f50%2Foutput%2Fgermline_single_sample_workflow%2F1b47bab1-eecd-4727-9589-b39b7d33d602%2Fcall-HaplotypeCaller4%2Fshard-32%2FSHIP6284138.vcf.gz.tbi', 'shardIndex': 32, 'size': 101317, 'storageClass': 'REGIONAL', 'timeCreated': '2021-10-28T18:54:49.778Z', 'timeCreatedEpoch': 1635447289.778, 'timeCreatedIso': '2021-10-28T18:54:49.778000+00:00', 'timeStorageClassUpdated': '2021-10-28T18:54:49.778Z', 'timeUpdatedEpoch': 1635447289.778, 'timeUpdatedIso': '2021-10-28T18:54:49.778000+00:00', 'trellisTask': 'gatk-5-dollar', 'trellisTaskId': '211027-212441-674-eb993f50', 'triggerOperation': 'finalize', 'updated': '2021-10-28T18:54:49.778Z', 'uri': 'gs://gbsc-gcp-project-mvp-from-personalis-phase3-data/DVALABP001859/SHIP6284138/gatk-5-dollar/211027-212441-674-eb993f50/output/germline_single_sample_workflow/1b47bab1-eecd-4727-9589-b39b7d33d602/call-HaplotypeCaller4/shard-32/SHIP6284138.vcf.gz.tbi', 'wdlCallAlias': 'HaplotypeCaller4'}}}, 'header': {'labels': ['Create', 'Generated', 'Relationship', 'CromwellStep', 'Output', 'Cypher', 'Query', 'Database', 'Result'], 'method': 'POST', 'previousEventId': '3297427071568348', 'resource': 'queryResult', 'seedId': '3295956991979012', 'sentFrom': 'trellis-db-query'}}

		data_str = json.dumps(data)
		data_utf8 = data_str.encode('utf-8')
		event = {'data': base64.b64encode(data_utf8)}

		self.message = trellis.TrellisMessage(event, mock_context)

	def test_all_activated_triggers(self):
		for name, trigger in ALL_TRIGGERS.items():
			result = trigger.check_all_conditions(message.header, message.body, message.node)
			if result == True:
				activated_triggers.append(name)
		assert len(activated_trigg) == 0