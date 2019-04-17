# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud beta functions deploy \
    wgs-9000-query-db-index \
    --project gbsc-gcp-project-mvp \
    --memory 128MB \
    --max-instances 40 \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-trellis \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ../../credentials/prod-wgs-9000.yaml 
