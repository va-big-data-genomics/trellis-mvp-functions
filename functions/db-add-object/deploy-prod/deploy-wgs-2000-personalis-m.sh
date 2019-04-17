gcloud functions deploy \
    wgs-2000-add-personalis-obj-to-db-m \
    --project gbsc-gcp-project-mvp \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-personalis \
    --trigger-event google.storage.object.metadataUpdate \
    --env-vars-file wgs-2000-credentials.yaml
