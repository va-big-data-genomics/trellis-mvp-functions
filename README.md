# trellis-mvp-wgs-35000
Trellis serverless data management framework for variant calling of VA MVP whole-genome sequencing data.

## v0.5.3
* Create Sample node from Json object & use as root for other objects from Personalis
* Add support for db-query to publish results to multiple topics
* Mark duplicate jobs as such in the database
* Don't create relationships between duplicate jobs & outputs
* User MERGE statement to create job nodes
* Change kill-duplicate-jobs to kill-job
