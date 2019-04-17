# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud functions deploy \
    wgs-9000-list-bucket-page \
    --project gbsc-gcp-project-mvp-dev \
    --memory 128MB \
    --entry-point main \
    --runtime python37 \
    --trigger-topic wgs-9000-bucket-page-tokens \
    --env-vars-file ../../credentials/dev-wgs-9000.yaml
