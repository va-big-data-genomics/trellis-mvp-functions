#!/usr/bin/env python3

from neo4j import GraphDatabase

samples_csv = "MVP-COVID-SHIP-IDs.csv"
doc_url = "https://docs.google.com/spreadsheets/d/1QjpHBU_G467y5zcbDSuh4XFdY88r_obK5nsN3zuxWdo/edit?usp=sharing"

samples = []
with open(samples_csv, 'r') as fh:
    for line in fh:
            samples.append({"sample": line.rstrip()})

cypher = """
    WITH $data AS samples
    UNWIND samples AS sample
    MATCH (n:Sample {sample: sample.sample})
    OPTIONAL MATCH (n)-[:HAS|INPUT_TO|OUTPUT|LED_TO*]->(v:Merged:Vcf)
    RETURN DISTINCT(n.sample) AS sample, v.size AS gvcf_size
"""

with driver.session() as session:
    results = session.run(cypher, data=samples[1:]).values()

with open('covid-samples.csv', 'w') as fh:
    for element in results:
        fh.write(f"element[0],element[1]")

