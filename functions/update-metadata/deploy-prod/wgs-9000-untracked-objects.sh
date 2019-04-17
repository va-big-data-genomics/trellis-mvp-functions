# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud functions deploy \
    wgs-9000-update-metadata \
    --project ***REMOVED*** \
    --memory 128MB \
    --entry-point main \
    --runtime python37 \
    --trigger-topic wgs-9000-untracked-objects \
    --env-vars-file ../../credentials/prod-wgs-9000.yaml
