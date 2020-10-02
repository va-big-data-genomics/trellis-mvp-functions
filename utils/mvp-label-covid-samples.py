#!/usr/bin/env python3

from neo4j import GraphDatabase

def get_csv_data(samples_csv):
    data = []
    with open(samples_csv, 'r') as fh:
        for line in fh:
                elements = line.rstrip().split(',')
                case_elements = elements[1].split(' ')
                if len(case_elements) == 2:
                    defined_by = case_elements[0]
                    case_definition = case_elements[1]
                    data.append({
                                 "shippingId": elements[0],
                                 "caseDefinition": case_definition,
                                 "caseDefinedBy": defined_by
                    })
                elif len(case_elements) == 3:
                    defined_by = case_elements[0]
                    case_confidence = case_elements[1]
                    case_definition = case_elements[2]
                    data.append({
                                    "shippingId": elements[0],
                                    "caseDefinition": case_definition,
                                    "caseDefinedBy": defined_by.
                                    "caseConfidence": case_confidence
                    })
    return data

def add_covid19_to_database(data, session):
    cypher = """
    WITH $data AS samples
    UNWIND samples AS sample
    MATCH (n:Sample {sample: sample.shippingId})
    SET n:Covid19, 
        n.cov19_caseDefinition = sample.caseDefinition,
        n.cov19_caseDefinedBy = sample.caseDefinedBy,
        n.cov19_caseConfidence = 
    RETURN n.sample, labels(n), n.covid19Positive
    """

    with driver.session() as session:
        results = session.run(cypher, data=data[1:]).values()

    with open('covid19-sample-properties.csv', 'w') as fh:
        for element in results:
            fh.write(f"element[0],element[1],element[2]")

def scrub_old_db_model():
    cypher = """
    MATCH (n:Sample:COVID19)
    REMOVE n.covid19Positive, n:COVID19
    RETURN n
    """