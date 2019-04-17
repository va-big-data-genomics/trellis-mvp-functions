# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud functions deploy \
    wgs-9000-db-query \
    --project gbsc-gcp-project-mvp \
    --memory 128MB \
    --entry-point query_db \
    --runtime python37 \
    --trigger-topic wgs-9000-db-queries \
    --env-vars-file ../../credentials/prod-wgs-9000.yaml
