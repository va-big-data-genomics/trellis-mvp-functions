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
            'trellisTaskId': groupdict['trellis_task_id'],
            'plate': groupdict['plate'],
    }


def cromwell_metadata_groupdict(db_dict, groupdict):
    return {
            'cromwellWorkflowName': groupdict['cromwell_workflow_name'],
            'cromwellWorkflowId': groupdict['cromwell_workflow_id'],
            'wdlCallAlias': groupdict['wdl_call_alias'],
    }


def shard_index_name_1(db_dict, groupdict):
    index = groupdict['shard_index']
    return {'shardIndex': int(index)}  


def read_group_name_1(db_dict, groupdict):
    index = db_dict['name'].split('_')[1]
    return {'readGroup': int(index)} 

# DEPRECATED WITH VERSION 1.2.3
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


class NodeKinds:

    def __init__(self):
        """Use to determine which kind of database node should be created.
        """

        self.match_patterns = {
            #"WGS35": [".*"],
            "Blob": [r"^(?P<plate>\w+)/(?P<sample>\w+)/(?P<trellis_task>\w+(?:-*\w*)+)/(?P<trellis_task_id>\w+(?:-\w+)+)/.*"],
            #"Cromwell": [r"^(?P<plate>\w+)/(?P<sample>\w+)/gatk-5-dollar/(?P<task_id>\w+(?:-\w+)+)/output/(?P<cromwell_workflow_name>\w+(?:_\w+)+)/(?P<cromwell_workflow_id>\w+(?:-\w+)+)/call-(?P<wdl_call_alias>\w+)/.*"],
            #"Gatk": [r"^(?P<plate>\w+)/(?P<sample>\w+)/gatk-5-dollar/.*"],
            "Vcf": [
                    ".*\\.vcf.gz$", 
                    ".*\\.vcf$",
            ],
            "Gvcf": [
                    ".*\\.g.vcf.gz$",
                    ".*\\.g.vcf$",
            ],
            #"Tbi": [".*\\.tbi$"],
            #"Gzipped": [".*\\.gz$"],
            #"Shard": [".*\\/shard-(?P<shard_index>\d+)\\/.*"],
            "Cram": [".*\\.cram$"], 
            #"Crai": [".*\\.crai$"],
            "Bam": [".*\\.bam$"], 
            #"Bai": [".*\\.bai$"],
            "Ubam": [".*\\.ubam$"],
            #"Aligned": [".*\\.aligned\\..*"],
            #"Filtered": [".*\\.filtered\\..*"],
            #"MarkedDuplicates": [".*\\.duplicates_marked\\..*"],
            #"Recalibrated": [".*\\.recalibrated\\..*", ".*\\.recal_.*"],
            #"Structured": [
            #               ".*\\.recal_data\\.csv$", 
            #               ".*\\.preBqsr.selfSM$", 
            #               ".*\\/sequence_grouping.*",
            #               ".*\\.duplicate_metrics$",
            #],
            #"Text": [
            #         ".*\\.recal_data\\.csv$", 
            #         ".*\\.preBqsr.selfSM$", 
            #         ".*\\.txt$", 
            #         ".*\\.duplicate_metrics$",
            #         ".*\\.validation_report$",
            #         ".*\\.tsv$",
            #],
            #"Log": [".*\\.log$"],
            #"Stderr": [".*\\/stderr$"],
            #"Stdout": [".*\\/stdout$"],
            "Log": [
                    ".*\\.log$",
                    ".*\\/stderr$",
                    ".*\\/stdout$"],
            #"Script": [".*\\/script$"],
            "Index": [
                      ".*\\.bai$",
                      ".*\\.tbi$",
                      ".*\\.crai$",
            ],
            #"Data": [
            #         ".*_data\\..*",
            #         ".*\\.recal_data\\.csv$", 
            #         ".*\\.preBqsr.selfSM$", 
            #         ".*\\/sequence_grouping.*",
            #         ".*\\.duplicate_metrics$",
            #         ".*\\.validation_report$",
            #         ".*\\.data\\..*",
            #],
            #"Unsorted": [".*\\.unsorted\\..*"],
            #"Sorted": [".*\\.sorted\\..*"],
            #"IntervalList": [".*\\.interval_list$"],
            "Json": [".*\\.json$"],
            #"Merged": [".*/call-MergeVCFs/.*"],
            "Fastqc": [".*/bam-fastqc/.*"],
            "Flagstat": [".*/flagstat/.*"],
            "Vcfstats": [".*/vcfstats/.*"],
            "TextToTable": [".*/text-to-table/.*"],
            "CheckContamination": [".*/call-CheckContamination/.*"],
        }

        self.label_functions = {
                                "Blob": [
                                         trellis_metadata_groupdict,
                                ],
                                "Shard": [
                                          shard_index_name_1,
                                ],
                                "Cromwell": [
                                         cromwell_metadata_groupdict,
                                ],
                                "Ubam": [
                                         read_group_name_1,
                                ],
        }
