# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud functions deploy \
    wgs-9000-match-object-patterns \
    --project gbsc-gcp-project-mvp-dev \
    --memory 128MB \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-dev-trellis \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ../../credentials/dev-wgs-9000.yaml
