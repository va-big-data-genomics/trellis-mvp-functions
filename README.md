# Trellis for VA MVP 100,000 WGS samples

## Context
As part of The Department of Veteran's Affairs (VA) Million Veterans Program, the VA has gathered genetic samples from 100,000s of donors within the VA health care system. The VA has, so far, contracted Personalis to perform whole-genome sequencing (WGS) on samples from 100,000 donors. The sequencing reads generated from these experiments are stored in Fastq format and streamed into buckets on the Google Cloud Platform. 

The value of performing whole-genome sequencing is that it generates sequencing reads from across all 3 billion positions in the human genome, while smaller-scale experiments such as genotyping arrays and whole-exome sequencing only assay small, select regions of the genome. However, only about 1-2% of the positions sequenced by whole-genome sequencing will be useful to researchers; these are the sites with nucleotides (or alleles) that are different from other individuals within the sample ancestral population. These single-nucleotide variants have the potential to contribute to the differences we see between people, and affect how they respond to medically relevant stimuli such as diet, viruses, bacteria, or other perturbations.

In order to discover these genetic variants, the sequencing reads must be processed and analyzed by a large set of applications that are organized as a single pipeline. We use the GATK best-practices pipeline, developed by the Broad Institute to do this variant calling for all of the autosomal chromosomes (1-22). This pipeline should be applied to the sequencing reads of every sample as soon as all its data has been uploaded to our cloud bucket. In addition to the GATK pipeline, we also run several applications to generate quality-control metrics, and are planning on adding additional workflows to detect variants in the mitochondrial genome (MT), sex chromosomes (X,Y) and structural variants (insertions, deletions, and copy number variants).

## Trellis
We developed as an event-driven data management system in order to automatically track the data objects associated with each sample and launch the appropriate workflows for different data types.

In order to automate the process of tracking these data objects and launching the appropriate workflows, we developed Trellis as an event-driven data management system

The challenge, is that is also generates a lot more data; about 70 GB of sequencing reads data per sample.



Trellis is an event-driven data management system that runs on serverless functions and uses a Neo4j graph database to store metadata.

Trellis serverless data management framework for variant calling of VA MVP whole-genome sequencing data.


## Update Notes
### v0.5.3
* Create Sample node from Json object & use as root for other objects from Personalis
* Add support for db-query to publish results to multiple topics
* Mark duplicate jobs as such in the database
* Don't create relationships between duplicate jobs & outputs
* User MERGE statement to create job nodes
* Change kill-duplicate-jobs to kill-job
