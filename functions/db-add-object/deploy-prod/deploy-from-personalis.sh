gcloud functions deploy add-db-entry-personalis \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-from-personalis \
    --trigger-event google.storage.object.finalize \
    --set-env-vars NEO4J_CREDENTIALS=gbsc-gcp-project-mvp-trellis-test:wakefield/credentials/neo4j-test.json
