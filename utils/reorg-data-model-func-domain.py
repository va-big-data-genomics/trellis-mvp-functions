######
## September 17, 2020
## Refactor card: https://trello.com/c/9iNF9y6Q/242-create-set-of-queries-to-refactor-database-to-allow-domain-based-queries
######

# (0) Relabel Sample node to PersonalisSequencing
relabel_sample_personalis_sequencing = """
    MATCH (s:Sample) WITH COLLECT(s) AS sequencing
    CALL apoc.refactor.rename.label(\"Sample\", \"PersonalisSequencing\", sequencing)
    YIELD committedOperations
    RETURN committedOperations
"""

# (1) Merge biological nodes from PersonalisSequencing
merge_biological_nodes = """
    CALL apoc.periodic.iterate(
        "MATCH (p:PersonalisSequencing) WHERE NOT (p)<-[:WAS_USED_BY]-(:Sample) RETURN p",
        "MERGE (p)<-[:WAS_USED_BY]-(:Sample {sample:p.sample})<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(:Genome:BiologicalOme {name:"genome"})",
        {batchSize: 1000, parallel:true}
    )
"""

####
## Refactor relationships
###

# (2) Change (:PersonalisSequencing)-[:HAS]->() relationship to [:GENERATED]
relabel_has_relationship_generated = """
    MATCH (s:PersonalisSequencing)-[r:HAS]->(n) WITH COLLECT(r) AS rels
    CALL apoc.refactor.rename.type(\"HAS\", \"GENERATED\", rels)
    YIELD committedOperations
    RETURN committedOperations
"""

# (3) Change [:INPUT_TO] relationship to [:WAS_USED_BY]
relabel_input_to_relationship_was_used_by = """
CALL apoc.refactor.rename.type(\"INPUT_TO\", \"WAS_USED_BY\")
"""

# (4) Change [:OUTPUT] relationship to [:GENERATED]
relabel_output_relationship_generated = """
CALL apoc.refactor.rename.type(\"OUTPUT\", \"GENERATED\")
"""

####
## Create relationships to (:Genome)
###

# (5) Merge (:Genome)-[:HAS_SEQUENCING_READS]->(:Fastq)
relate_fastqs = """
    CALL apoc.periodic.iterate(
        "MATCH (p:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) RETURN p, g",
        "MATCH (p)-[:WAS_USED_BY|GENERATED*3]->(f:Fastq) MERGE (g)-[:HAS_SEQUENCING_READS]->(f)",
        {batchSize: 200, parallel:true}
    )
"""

relate_fastqs_2 = """
    CALL apoc.periodic.iterate(
        "MATCH (f:Fastq) RETURN f",
        "MATCH (:Sample {sample:f.sample})<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) MERGE (g)-[:HAS_SEQUENCING_READS]->(f)",
        {batchSize: 200, parallel: true}
    )
"""

# (6) Merge (:Genome)-[:HAS_SEQUENCING_READS]->(:Cram)
relate_crams = """
    CALL apoc.periodic.iterate(
    "MATCH (p:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) RETURN p, g",
    "MATCH (p)-[:WAS_USED_BY|GENERATED|LED_TO*]->(c:Cram) MERGE (g)-[:HAS_SEQUENCING_READS]->(c)",
    {batchSize: 200, parallel:true}
)
"""

relate_crams_2 = """
    CALL apoc.periodic.iterate(
        "MATCH (c:Cram) RETURN c",
        "MATCH (:Sample {sample:c.sample})<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) MERGE (g)-[:HAS_SEQUENCING_READS]->(c)",
        {batchSize: 200, parallel: true}
    )
"""

# (7) Merge (:Genome)-[:HAS_VARIANT_CALLS]->(:Merged:Vcf)
relate_gvcfs = """
    CALL apoc.periodic.iterate(
        "MATCH (p:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) RETURN p, g",
        "MATCH (p)-[:WAS_USED_BY|GENERATED|LED_TO*15..30]->(v:Merged:Vcf) MERGE (g)-[:HAS_VARIANT_CALLS]->(v)",
        {batchSize: 200, parallel:true}
    )
"""

relate_gvcfs_2 = """
    CALL apoc.periodic.iterate(
        "MATCH (v:Merged:Vcf) RETURN v",
        "MATCH (:Sample {sample:v.sample})<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) MERGE (g)-[:HAS_VARIANT_CALLS]->(v)",
        {batchSize: 200, parallel: true}
    )
"""

# (8) Merge (:Genome)-[:HAS_QC_DATA]->(:Fastqc)
relate_fastqc_to_ome = """
CALL apoc.periodic.iterate(
    "MATCH (p:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) RETURN p, g",
    "MATCH (p)-[:WAS_USED_BY|GENERATED|LED_TO*15..27]->(qc:Fastqc:Text:Data) MERGE (g)-[:HAS_QC_DATA {ontology:\"bioinformatics\"}]->(qc)",
    {batchSize: 200, parallel:true}
)
"""

relate_fastqc_2 = """
    CALL apoc.periodic.iterate(
        "MATCH (f:Fastqc:Text:Data) RETURN f",
        "MATCH (:Sample {sample:f.sample})<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) MERGE (g)-[:HAS_QC_DATA {ontology:\"bioinformatics\"}]->(f)",
        {batchSize: 200, parallel: true}
    )
"""

# (9) Merge (:Genome)-[:HAS_QC_DATA]->(:Flagstat)
relate_flagstat_to_ome = """
CALL apoc.periodic.iterate(
    "MATCH (p:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) RETURN p, g",
    "MATCH (p)-[:WAS_USED_BY|GENERATED|LED_TO*15..27]->(qc:Flagstat:Text:Data) MERGE (g)-[:HAS_QC_DATA {ontology:\"bioinformatics\"}]->(qc)",
    {batchSize: 200, parallel:true}
)
"""

relate_flagstat_2 = """
    CALL apoc.periodic.iterate(
        "MATCH (f:Flagstat:Text:Data) RETURN f",
        "MATCH (:Sample {sample:f.sample})<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) MERGE (g)-[:HAS_QC_DATA {ontology:\"bioinformatics\"}]->(f)",
        {batchSize: 200, parallel: true}
    )
"""

# (10) Merge (:Genome)-[:HAS_QC_DATA]->(:Vcfstats)
relate_vcfstats_to_ome = """
CALL apoc.periodic.iterate(
    "MATCH (p:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) RETURN p, g",
    "MATCH (p)-[:WAS_USED_BY|GENERATED|LED_TO*18..30]->(qc:Vcfstats:Text:Data) MERGE (g)-[:HAS_QC_DATA {ontology:\"bioinformatics\"}]->(qc)",
    {batchSize: 200, parallel:true}
)
"""

relate_vcfstats_2 = """
    CALL apoc.periodic.iterate(
        "MATCH (v:Vcfstats:Text:Data) RETURN v",
        "MATCH (:Sample {sample:v.sample})<-[:GENERATED]-(:Person)-[:HAS_BIOLOGICAL_OME]->(g:Genome) MERGE (g)-[:HAS_QC_DATA {ontology:\"bioinformatics\"}]->(v)",
        {batchSize: 200, parallel: true}
    )
"""

# (11) Create (:Cram)-[:HAS_INDEX]->(:Crai)
relate_cram_to_crai = """
CALL apoc.periodic.iterate(
    "MATCH (g:Genome)-[:HAS_SEQUENCING_READS]->(cram:Cram)<-[:GENERATED]-(step:CromwellStep) RETURN cram, step",
    "MATCH (step)-[:GENERATED]->(crai:Crai) MERGE (cram)-[:HAS_INDEX {ontology:\"bioinformatics\"}]->(crai)",
    {batchSize: 200, parallel:true}
)
"""

# (12) Create (:Merged:Vcf)-[:HAS_INDEX]->(:Tbi)
relate_vcf_to_tbi = """
CALL apoc.periodic.iterate(
    "MATCH (g:Genome)-[:HAS_VARIANT_CALLS]->(vcf:Merged:Vcf)<-[:GENERATED]-(step:CromwellStep) RETURN vcf, step",
    "MATCH (step)-[:GENERATED]->(tbi:Tbi) MERGE (vcf)-[:HAS_INDEX {ontology:\"bioinformatics\"}]->(tbi)",
    {batchSize: 200, parallel:true}
)
"""

# Move Covid19 metadata from :Sequencing to :Person node
move_covid_to_person = """
CALL apoc.periodic.iterate(
    "MATCH (s:PersonalisSequencing:COVID19) RETURN s",
    "MATCH (p:Person)-[*2]->(s) SET p:Covid19, 
     p.cov19_caseDefinition = s.cov19_caseDefinition,
     p.cov19_caseDefinedBy = s.cov19_caseDefinedBy,
     p.cov19_caseConfidence = s.cov19_caseConfidence,
     REMOVE s:COVID19, 
     s.cov19_caseDefinition,
     s.cov19_caseDefinedBy,
     s.cov19_caseConfidence",
    {batchSize: 200, parallel: true}
)
"""

#####
# Initiate data consolidation
#####

validate_sample_relationships = """
MATCH (s:Sample)-[:HAS_BIOLOGICAL_OME]->(b:BiologicalOme)
WITH s.sample AS sample, b
LIMIT 1
MATCH p=(b)-[*1..2]-(blob:Blob)
WHERE ALL (r in relationships(p) WHERE r.ontology="bioinformatics")
RETURN labels(blob), COUNT(blob)
"""

get_inessential_blobs = """
    MATCH (s:Sample)-[:HAS_BIOLOGICAL_OME]->(o:BiologicalOme)-[:HAS|INPUT_TO|OUTPUT|LED_TO*]->(b:Blob)
    WHERE s.sample = {sample}
    AND o.name = {name}
    WITH DISTINCT(b) AS blobs
    MATCH p=(b)-[r {ontology: "bioinformatics"}*1..2]-(:BiologicalOme)
    WHERE NONE (r in relationships(p) WHERE r.ontology="bioinformatics")
    RETURN \"gs://\" + b.bucket + \"/\" + b.path AS uri
"""

get_inessential_blobs = """
    MATCH (s:Sample)-[:HAS|INPUT_TO|OUTPUT|LED_TO*]->(b:Blob)
    WHERE s.sample = "SHIP4946367"
    WITH COLLECT(DISTINCT(b)) AS all_blobs
    UNWIND all_blobs AS b
    MATCH p=(b)-[*1..2]-(:BiologicalOme)
    WHERE ALL (r in relationships(p) WHERE r.ontology="bioinformatics")
    WITH all_blobs, COLLECT(b) AS essential_blobs
    UNWIND all_blobs AS b
    MATCH (b)
    WHERE NOT b IN essential_blobs
    RETURN COUNT(b)
"""
