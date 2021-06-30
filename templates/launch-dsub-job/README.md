# Creating a Trellis function for launching dsub jobs

## Choose a descriptive name for the dsub job you are going to launch
The name should accurately describe the job and may be the name of the tool ("vcfstats") or the tool and function ("samtools-index"). We will call this the _job name_ and use "cnvnator" in this example.

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




