# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud beta functions deploy \
    wgs-9000-db-query-index \
    --project gbsc-gcp-project-mvp-dev \
    --memory 128MB \
    --max-instances 20 \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-dev-trellis \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ../../credentials/dev-wgs-9000.yaml 
