import re
import pdb
import json
import pytz
import iso8601

from datetime import datetime

from google.cloud import storage

## Functions for paring custom metadata from blob metadata

def trellis_metadata_groupdict(db_dict, groupdict):
    return {
            'sample': groupdict['sample'],
            'trellisTask': groupdict['trellis_task'],
            'taskId': groupdict['task_id'],
    }


def workflow_path_4(db_dict, groupdict):
    value = db_dict['path'].split('/')[4] 
    return {'gatkWorkflow': str(value)}


def task_path_5(db_dict, groupdict):
    value = db_dict['path'].split('/')[5] 
    task = value.split('-')[1]
    return {'gatkTask': str(task)}


def shard_index_name_1(db_dict, groupdict):
    index = groupdict['shard_index']
    return {'shardIndex': int(index)}  


def read_group_name_1(db_dict):
    index = db_dict['name'].split('_')[1]
    return {'readGroup': int(index)} 


def get_metadata_from_all_json(db_dict, groupdict):

    meta_bucket = db_dict['bucket']

    meta_blob_path = db_dict['dirname'].split('/')[:-1]
    meta_blob_path.extend(['metadata', 'all-objects.json'])
    meta_blob_path = '/'.join(meta_blob_path)

    metadata_str = storage.Client() \
        .get_bucket(meta_bucket) \
        .blob(meta_blob_path) \
        .download_as_string()
    metadata = json.loads(metadata_str)
    return metadata

# Relationship functions
def relate_job_to_output(db_dict):

    query = (
             f"MATCH (j:Job {{taskId:\"{db_dict['taskId']}\"}), " +
             f"(b:Blob {{taskID:\"{db_dict['taskId']}\", " +
                        "id:\"{db_dict['id']\"}})" +
             f"CREATE (j)-[:OUTPUT]->(b) " +
              "RETURN b")
    return query


class NodeKinds:

    def __init__(self):
        """Use to determine which kind of database node should be created.
        """

        self.match_patterns = {
            "WGS35": [".*"],
            "Blob": [r"(?P<sample>\w+)/(?P<trellis_task>.*)/(?P<task_id>.*)/output/.*"],
            "Gatk": ["\w+/gatk-5-dollar/.*"],
            "Vcf": [
                    ".*\\.vcf.gz$", 
                    ".*\\.vcf$",
            ],
            "Tbi": [".*\\.tbi$"],
            "Gzipped": [".*\\.gz$"],
            "Shard": [".*\\/shard-(?P<shard_index>\d+)\\/.*"],
            "Cram": [".*\\.cram$"], 
            "Crai": [".*\\.crai$"],
            "Bam": [".*\\.bam$"], 
            "Bai": [".*\\.bai$"],
            "Ubam": [".*\\.ubam$"],
            "Aligned": [".*\\.aligned\\..*"],
            "Filtered": [".*\\.filtered\\..*"],
            "MarkedDuplicates": [".*\\.duplicates_marked\\..*"],
            "Recalibrated": [".*\\.recalibrated\\..*", ".*\\.recal_.*"],
            "Structured": [
                           ".*\\.recal_data\\.csv$", 
                           ".*\\.preBqsr.selfSM$", 
                           ".*\\/sequence_grouping.*",
                           ".*\\.duplicate_metrics$",
            ],
            "Text": [
                     ".*\\.recal_data\\.csv$", 
                     ".*\\.preBqsr.selfSM$", 
                     ".*\\.txt$", 
                     ".*\\.duplicate_metrics$",
                     ".*\\.validation_report$",
            ],
            "Log": [".*\\.log$"],
            "Stderr": [".*\\/stderr$"],
            "Stdout": [".*\\/stdout$"],
            "Script": [".*\\/script$"],
            "Index": [
                      ".*\\.bai$",
                      ".*\\.tbi$",
                      ".*\\.crai$",
            ],
            "Data": [
                     ".*_data\\..*",
                     ".*\\.recal_data\\.csv$", 
                     ".*\\.preBqsr.selfSM$", 
                     ".*\\/sequence_grouping.*",
                     ".*\\.duplicate_metrics$",
                     ".*\\.validation_report$",
            ],
            "Unsorted": [".*\\.unsorted\\..*"],
            "Sorted": [".*\\.sorted\\..*"],
            "IntervalList": [".*\\.interval_list$"],
            "Json": [".*\\.json$"],
        }

        self.label_functions = {
                                "Blob": [
                                         trellis_metadata_groupdict,
                                ],
                                "Shard": [shard_index_name_1],
                                "Gatk": [
                                         workflow_path_4,
                                         task_path_5,
                                ],
                                "Ubam": [read_group_name_1],
        }

class RelationshipKinds:

    def __init__(self):

        self.shipping_properties = {
                                 "taskId": [relate_job_to_output],
        }
