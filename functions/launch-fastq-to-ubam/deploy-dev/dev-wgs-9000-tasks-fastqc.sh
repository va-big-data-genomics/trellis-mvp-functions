# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud functions deploy \
    wgs-9000-launch-fastqc \
    --project ***REMOVED***-dev \
    --memory 128MB \
    --entry-point launch_fastqc \
    --runtime python37 \
    --trigger-topic wgs-9000-tasks-fastqc \
    --env-vars-file ../../credentials/dev-wgs-9000.yaml
