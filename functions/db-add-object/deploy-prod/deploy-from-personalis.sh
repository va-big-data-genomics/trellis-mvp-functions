gcloud functions deploy add-db-entry-personalis \
    --entry-point main \
    --runtime python37 \
    --trigger-resource ***REMOVED***-from-personalis \
    --trigger-event google.storage.object.finalize \
    --set-env-vars NEO4J_CREDENTIALS=***REMOVED***-trellis-test:wakefield/credentials/neo4j-test.json
