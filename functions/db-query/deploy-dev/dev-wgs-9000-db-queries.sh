# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud beta functions deploy \
    wgs-9000-db-query \
    --project gbsc-gcp-project-mvp-dev \
    --memory 128MB \
    --max-instances 20 \
    --entry-point query_db \
    --runtime python37 \
    --trigger-topic wgs-9000-db-queries \
    --env-vars-file ../../credentials/dev-wgs-9000.yaml
