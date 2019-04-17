# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud beta functions deploy \
    wgs-9000-launch-fastqc \
    --project ***REMOVED*** \
    --memory 128MB \
    --max-instances 10 \
    --entry-point launch_fastqc \
    --runtime python37 \
    --trigger-topic wgs-9000-tasks-fastqc \
    --env-vars-file ../../credentials/prod-wgs-9000.yaml
