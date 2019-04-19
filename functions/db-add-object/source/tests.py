import os

import main as main

# Set environment variables
#os.environ['NEO4J_URL'] = "https://35.247.31.130:7473"
#os.environ['NEO4J_USER'] = "neo4j"
#os.environ['NEO4J_PASSPHRASE'] = "IxH3JD_LNPBQq398xSrPifatw7Ha_SSX"
#os.environ['GOOGLE_CLOUD_PROJECT'] = "gbsc-gcp-project-mvp-dev"
#os.environ['NEW_DB_NODE_TOPIC'] = "trellis-unit-tests"
#os.environ['DATA_GROUP'] = 'wgs-35000'


def test_add_fastqc_node():
    event = {
             "bucket": "gbsc-gcp-project-mvp-dev-from-personalis-qc",
             "name": "dsub/fastqc-bam/fastqc/objects/SHIP3935743_chromosome_10_alignments.bam.fastqc_data.txt"
    }
    context = None
    message = main.add_obj_to_db(event, context)
    print(message)
