gcloud functions deploy \
    add-db-entry-meta-phase-2-data \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-phase-2-data \
    --trigger-event google.storage.object.metadataUpdate \
    --env-vars-file wgs-2000-credentials.yaml
