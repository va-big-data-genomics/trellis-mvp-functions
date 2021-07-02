# Creating a Trellis function for launching dsub jobs

## Choose a descriptive name for the dsub job you are going to launch
The name should accurately describe the job and may be the name of the tool ("vcfstats") or the tool and function ("samtools-index"). We will call this the _job name_ and use "cnvnator" in this example.

## Craft the database query

Because the Neo4j database is the only stateful component of Trellis, all workflow decisions are mediated through database queries. Once a dsub job trigger is activated, it sends a Cypher query to the database that will get the metadata of all the input objects for that job, and send those metadata to the function that will launch the job.

When writing your query, you should consider several factors:

	1. Which object nodes are required as input to my job?
	2. p 

## Create a database trigger

1. Name your trigger
    
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

2. Define required node/header labels
	Node and message header labels are the primary indicators of whether to activate a trigger. Node labels are used primarily for event-driven triggers that are activated by database changes. Header labels come from the header section of the incoming Pub/Sub message. These are used primarily for request-driven triggers activated by user requests, because these requests are not associated with a database node.

3. Add trigger conditions
	
	Trigger conditions defined the metadata conditions that must be met before activating the trigger. Conditions are defined as python statements that will be evaluated in an "if" statement. If any do not return a positive result (e.g. 0, None, False), the trigger will not be activated and the database query will not be run.


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




