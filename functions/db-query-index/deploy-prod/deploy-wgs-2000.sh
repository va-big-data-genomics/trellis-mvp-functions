gcloud functions deploy \
    query-db-index-wgs-2000 \
    --project gbsc-gcp-project-mvp \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-trellis-test \
    --trigger-event google.storage.object.finalize \
    --env-vars-file wgs-2000-credentials.yaml 
