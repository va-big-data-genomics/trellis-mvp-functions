if __name__ == "__main__":
    # Run unit tests in local
    PROJECT_ID = "***REMOVED***-dev"
    TOPIC = "function-test"
    DATA_GROUP = 'wgs35'

    PUBLISHER = pubsub.PublisherClient()
    TOPIC_PATH = 'projects/{id}/topics/{topic}'.format(
                                                       id = PROJECT_ID,
                                                       topic = TOPIC)

    # gatk-5-dollar job
    """
    data = {
            "header": {
                       "method": "POST", 
                       "labels": ["Job", "Cromwell", "Command", "Args", "Inputs"], 
                       "resource": "job-metadata"
                      }, 
                      "body": {
                               "node": {
                                        "provider": "google-v2", 
                                        "user": "trellis", 
                                        "zones": "us-west1*", 
                                        "project": "***REMOVED***-dev", 
                                        "min_cores": 1, 
                                        "min_ram": 6.5, 
                                        "preemptible": true, 
                                        "boot_disk_size": 20, 
                                        "image": "gcr.io/***REMOVED***-dev/***REMOVED***/wdl_runner:latest", 
                                        "logging": "gs://***REMOVED***-dev-from-personalis-gatk-logs/SHIP4946367/fastq-to-vcf/gatk-5-dollar/logs", 
                                        "disk-size": 1000, 
                                        "command": "java -Dconfig.file=${CFG} -Dbackend.providers.JES.config.project=${MYproject} -Dbackend.providers.JES.config.root=${ROOT} -jar /cromwell/cromwell.jar run ${WDL} --inputs ${INPUT} --options ${OPTION}", 
                                        "inputs": {
                                                   "CFG": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf", 
                                                   "OPTION": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json", 
                                                   "WDL": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl", 
                                                   "SUBWDL": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl", 
                                                   "INPUT": "gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json"
                                                  }, 
                                        "envs": {
                                                 "MYproject": "***REMOVED***-dev", 
                                                 "ROOT": "gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output"
                                                }, 
                                        "dry-run": True, 
                                        "labels": ["Job", "Cromwell"], 
                                        "input_CFG": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf", 
                                        "input_OPTION": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json", 
                                        "input_WDL": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl", 
                                        "input_SUBWDL": "gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl", 
                                        "input_INPUT": "gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json", 
                                        "env_MYproject": "***REMOVED***-dev", 
                                        "env_ROOT": "gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output"
                               }, 
                               "perpetuate": {
                                              "relationships": {
                                                                "to-node": {
                                                                            "INPUT_TO": [
                                                                                         {"basename": "SHIP4946367_2.ubam", "bucket": "***REMOVED***-dev-from-personalis-gatk", "contentType": "application/octet-stream", "crc32c": "ojStVg==", "dirname": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output", "etag": "CJTpxe3ynuICEAM=", "extension": "ubam", "generation": "1557970088457364", "id": "***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam/1557970088457364", "kind": "storage#object", "labels": ["WGS35", "Blob", "Ubam"], "md5Hash": "opGAi0f9olAu4DKzvYiayg==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam?generation=1557970088457364&alt=media", "metageneration": "3", "name": "SHIP4946367_2", "path": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam", "sample": "SHIP4946367", "selfLink": "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam", "size": 16886179620, "storageClass": "REGIONAL", "timeCreated": "2019-05-16T01:28:08.455Z", "timeCreatedEpoch": 1557970088.455, "timeCreatedIso": "2019-05-16T01:28:08.455000+00:00", "timeStorageClassUpdated": "2019-05-16T01:28:08.455Z", "timeUpdatedEpoch": 1558045261.522, "timeUpdatedIso": "2019-05-16T22:21:01.522000+00:00", "trellisTask": "fastq-to-ubam", "trellisWorkflow": "fastq-to-vcf", "updated": "2019-05-16T22:21:01.522Z"}, 
                                                                                         {"basename": "SHIP4946367_0.ubam", "bucket": "***REMOVED***-dev-from-personalis-gatk", "contentType": "application/octet-stream", "crc32c": "ZaJM+g==", "dirname": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output", "etag": "CM+sxKDynuICEAY=", "extension": "ubam", "generation": "1557969926952527", "id": "***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam/1557969926952527", "kind": "storage#object", "labels": ["WGS35", "Blob", "Ubam"], "md5Hash": "Tgh+eyIiKe8TRWV6vohGJQ==", "mediaLink": "https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam?generation=1557969926952527&alt=media", "metageneration": "6", "name": "SHIP4946367_0", "path": "SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam", "sample": "SHIP4946367", "selfLink": "https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam", "size": 16871102587, "storageClass": "REGIONAL", "timeCreated": "2019-05-16T01:25:26.952Z", "timeCreatedEpoch": 1557969926.952, "timeCreatedIso": "2019-05-16T01:25:26.952000+00:00", "timeStorageClassUpdated": "2019-05-16T01:25:26.952Z", "timeUpdatedEpoch": 1558045265.901, "timeUpdatedIso": "2019-05-16T22:21:05.901000+00:00", "trellisTask": "fastq-to-ubam", "trellisWorkflow": "fastq-to-vcf", "updated": "2019-05-16T22:21:05.901Z"}
                                                                            ]
                                                                }
                                              }
                               }
                      }
            }
    """
    data = {'header': {'method': 'POST', 'labels': ['Job', 'Cromwell', 'Command', 'Args', 'Inputs'], 'resource': 'job-metadata'}, 'body': {'node': {'provider': 'google-v2', 'user': 'trellis', 'zones': 'us-west1*', 'project': '***REMOVED***-dev', 'minCores': 1, 'minRam': 6.5, 'preemptible': True, 'bootDiskSize': 20, 'image': 'gcr.io/***REMOVED***-dev/***REMOVED***/wdl_runner:latest', 'logging': 'gs://***REMOVED***-dev-from-personalis-gatk-logs/SHIP4946367/fastq-to-vcf/gatk-5-dollar/logs', 'diskSize': 1000, 'command': 'java -Dconfig.file=${CFG} -Dbackend.providers.JES.config.project=${MYproject} -Dbackend.providers.JES.config.root=${ROOT} -jar /cromwell/cromwell.jar run ${WDL} --inputs ${INPUT} --options ${OPTION}', 'inputs': {'CFG': 'gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf', 'OPTION': 'gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json', 'WDL': 'gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl', 'SUBWDL': 'gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl', 'INPUT': 'gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json'}, 'envs': {'MYproject': '***REMOVED***-dev', 'ROOT': 'gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output'}, 'dryRun': True, 'labels': ['Job', 'Cromwell'], 'input_CFG': 'gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/google-adc.conf', 'input_OPTION': 'gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/generic.google-papi.options.json', 'input_WDL': 'gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/fc_germline_single_sample_workflow.wdl', 'input_SUBWDL': 'gs://***REMOVED***-dev-trellis/workflow-inputs/gatk-mvp/gatk-mvp-pipeline/tasks_pipelines/*.wdl', 'input_INPUT': 'gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/inputs/inputs.json', 'env_MYproject': '***REMOVED***-dev', 'env_ROOT': 'gs://***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/gatk-5-dollar/output'}, 'perpetuate': {'relationships': {'to-node': {'INPUT_TO': [{'basename': 'SHIP4946367_2.ubam', 'bucket': '***REMOVED***-dev-from-personalis-gatk', 'contentType': 'application/octet-stream', 'crc32c': 'ojStVg==', 'dirname': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output', 'etag': 'CJTpxe3ynuICEAM=', 'extension': 'ubam', 'generation': '1557970088457364', 'id': '***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam/1557970088457364', 'kind': 'storage#object', 'labels': ['WGS35', 'Blob', 'Ubam'], 'md5Hash': 'opGAi0f9olAu4DKzvYiayg==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam?generation=1557970088457364&alt=media', 'metageneration': '3', 'name': 'SHIP4946367_2', 'path': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_2.ubam', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_2.ubam', 'size': 16886179620, 'storageClass': 'REGIONAL', 'timeCreated': '2019-05-16T01:28:08.455Z', 'timeCreatedEpoch': 1557970088.455, 'timeCreatedIso': '2019-05-16T01:28:08.455000+00:00', 'timeStorageClassUpdated': '2019-05-16T01:28:08.455Z', 'timeUpdatedEpoch': 1558045261.522, 'timeUpdatedIso': '2019-05-16T22:21:01.522000+00:00', 'trellisTask': 'fastq-to-ubam', 'trellisWorkflow': 'fastq-to-vcf', 'updated': '2019-05-16T22:21:01.522Z'}, {'basename': 'SHIP4946367_0.ubam', 'bucket': '***REMOVED***-dev-from-personalis-gatk', 'contentType': 'application/octet-stream', 'crc32c': 'ZaJM+g==', 'dirname': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output', 'etag': 'CM+sxKDynuICEAY=', 'extension': 'ubam', 'generation': '1557969926952527', 'id': '***REMOVED***-dev-from-personalis-gatk/SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam/1557969926952527', 'kind': 'storage#object', 'labels': ['WGS35', 'Blob', 'Ubam'], 'md5Hash': 'Tgh+eyIiKe8TRWV6vohGJQ==', 'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam?generation=1557969926952527&alt=media', 'metageneration': '6', 'name': 'SHIP4946367_0', 'path': 'SHIP4946367/fastq-to-vcf/fastq-to-ubam/output/SHIP4946367_0.ubam', 'sample': 'SHIP4946367', 'selfLink': 'https://www.googleapis.com/storage/v1/b/***REMOVED***-dev-from-personalis-gatk/o/SHIP4946367%2Ffastq-to-vcf%2Ffastq-to-ubam%2Foutput%2FSHIP4946367_0.ubam', 'size': 16871102587, 'storageClass': 'REGIONAL', 'timeCreated': '2019-05-16T01:25:26.952Z', 'timeCreatedEpoch': 1557969926.952, 'timeCreatedIso': '2019-05-16T01:25:26.952000+00:00', 'timeStorageClassUpdated': '2019-05-16T01:25:26.952Z', 'timeUpdatedEpoch': 1558045265.901, 'timeUpdatedIso': '2019-05-16T22:21:05.901000+00:00', 'trellisTask': 'fastq-to-ubam', 'trellisWorkflow': 'fastq-to-vcf', 'updated': '2019-05-16T22:21:05.901Z'}]}}}}}
    data = json.dumps(data).encode('utf-8')
    event = {'data': base64.b64encode(data)}   
    context = {'event_id': 'Test'}

    write_job_node_query(event, context)
