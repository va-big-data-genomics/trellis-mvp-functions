# Creating a Trellis function for launching dsub jobs

## Choose a descriptive name for the dsub job you are going to launch
The name should accurately describe the job and may be the name of the tool ("vcfstats") or the tool and function ("samtools-index"). We will call this the _job name_ and use "cnvnator" in this example.

## Determine the inputs required to run your job

These should all be inputs that are represented as nodes in the database, for each sample.

1. Determine which types of objects are necessary inputs (e.g. Cram/Crai, Fastq, gVcg/Tbi)
2. Query the database to discover how they are connected to other nodes
3. Think about how you are going to query these inputs. What database relationships are necessary?
4. Study the UML sequence diagrams for your input types. Based on the relationships you need, where should your job be added?

## Add your job to the UML sequence diagram(s)

### Background
For each cardinal data object that triggers bioinformatics jobs (e.g. Fastq, Cram, gVcf) we generate a UML sequence diagram representing how the creation of that node object initiates a sequence of database triggers to launch downstream jobs. The diagram is useful for understanding

1. Which jobs are triggered by an object node?
2. Which prerequisites are necessary for meeting the conditions of launching a job?
    1. Which object nodes need to be present in the database?
    2. Which kinds of relationships need to be present in the database?
3. Where race conditions can occur?

In addition to adding your job events to the object diagrams, you may want to create a separate diagram just to describe the events required to trigger your job. 

Open up your diagramming tool of choice. I used Lucid (paid) or draw.io (free). If you are not familiar with UML sequence diagrams you can check out this video or find your own.

    How to Make a UML Sequence Diagram: https://www.youtube.com/watch?v=pCK6prSq8aw)

Example Trellis sequence diagram:

    ```
    /template/trellis-cram-sequence.pdf
    ```

## Create a database trigger

### Name your trigger
    
	Format of a database trigger can be though of in (3) sections:
    
    1. Launch/Request
    2. Job name
    3. Specifics (optional)

    In our case, we will use the name "RequestCnvnator"

    ### Launch/Request:
    Most triggers are used to launch dsub jobs. In that case the first word should be Request/Launch. "Launch" indicates the trigger is activated by database event (e.g. new node added) while "Request" indicates the trigger is activated by a user request, usually delivered by Cloud Scheduler job.

    We are using "Request" because this trigger will be activated
    by user requests.

    ### Job name: 
    Job name should describe that will be perfomed by the dsub task. 
    We will use "Cnvnator"

    ### Specifics:
    Specifics usually describe how this job deviates from the basic form of the job. Since ours _is_ the basic form of the job we will not add any words describing specifics. But, for instance, if we wanted to create a trigger to only launch Cnvnator jobs for genomes in the Covid-19 cohort we might add the word "Covid19" to our trigger name to make it "RequestCnvnatorCovid19".

### Define required node/header labels
	Node and message header labels are the primary indicators of whether to activate a trigger. Node labels are used primarily for event-driven triggers that are activated by database changes. Header labels come from the header section of the incoming Pub/Sub message. These are used primarily for request-driven triggers activated by user requests, because these requests are not associated with a database node.

### Add trigger conditions
	
	Trigger conditions defined the metadata conditions that must be met before activating the trigger. Conditions are defined as python statements that will be evaluated in an "if" statement. If any do not return a positive result (e.g. 0, None, False), the trigger will not be activated and the database query will not be run.

### Craft the database query

    Because the Neo4j database is the only stateful component of Trellis, all workflow decisions are mediated through database queries. Once a dsub job trigger is activated, it sends a Cypher query to the database that will get the metadata of all the input objects for that job, and send those metadata to the function that will launch the job.

    When writing your query, you should consider several factors:

        1. Which object nodes are required as input to my job?
        2. Are multiple nodes required as input?
        3. Are they related and which relationships are required for querying them?

    For our CNVnator job, the required nodes are the cram object and its associated crai index. Trellis already has a database trigger programmed to create a functional (:Cram)-[:INDEX]->(:Crai) relationship between these two nodes. Because the Cram is deemed to be the primary node in that relationship, it is the node that is returned from the query that created the [:INDEX] relationship. Thus, we are going to specify the Cram node as the activator for this trigger, and set a condition that the node must be received as part of the result of a query connecting the (:Cram) to the (:Crai) using an [:INDEX] relationships. These are indicated by the message header labels.

## Create your function directory from template
Copy this "launch-dsub-job" directory from templates/launch-dsub-job to functions/launch-<_job name_>

```
# From the root directory of this repository
cp -r templates/launch-dsub-job functions/launch-cnvnator

cd functions/launch-cnvnator
```

# Do this after creating main.py
## Update the cloudbuild.yaml file
The function will be deployed to the Google Cloud Platform project using Cloud Build, a tool for automatically deploying functions when they are updated in GitHub. The cloudbuild.yaml defines the Cloud Build deployment instructions.

Use your preferred text editor to open cloudbuild.yaml and do the following:

* line 4: replace <INSERT_JOB_NAME> with your job name ("cnvnator")
* line 6: replace <INSERT_JOB_NAME> with your job name
* line 9: replace <INSERT_MAIN_METHOD_NAME> with your job name




