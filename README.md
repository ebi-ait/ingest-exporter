[![Docker Repository on Quay](https://quay.io/repository/ebi-ait/ingest-exporter/status "Docker Repository on Quay")](https://quay.io/repository/ebi-ait/ingest-exporter)

# ingest-exporter

Component that handles the generation of assay manifests for archiving and exporting of HCA metadata and data files to DCP
 
This component listens for messages from the Ingest Core using RabbitMQ. When a submission is valid and complete (i.e. all data files have been uploaded to the upload area), Ingest Core will notify this component and this will trigger the exporting or "assay" manifests generation. 

## Message listeners

### Terra Listener

![Terra Listener](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/ebi-ait/ingest-exporter/dcp-692_update-readme/docs/exporting-to-terra.diag)

### Assay Manifest Generator Listener

This listener creates a "bundle" manifest entity in Ingest Core which contains all the metadata uuids related to an assay process. The bundle manifest entity was created for a different purpose before but was reused to aid in conversion of HCA entities to the EBI Archive entities.
It's more appropriate to call it "Assay" Manifest instead for its role in archiver conversion.

![Assay Manifest Generator Listener](http://www.plantuml.com/plantuml/proxy?cache=no&src=https://raw.githubusercontent.com/ebi-ait/ingest-exporter/dcp-692_update-readme/docs/generating-assay-manifests.diag)

## How to run locally

```bash
python -mvenv .venv
source .venv/bin/activate
pip install -r requirements.txt
python exporter.py
```

## How to run locally and hijack a given environment
1. [Port forward rabbitMQ in environment of choice](https://github.com/ebi-ait/ingest-kube-deployment#accessing-rabbitmq-management-ui)
1. Bring down deployment of ingest-exporter in environment of choice
    - `kubectl delete deployment ingest-exporter`
1. Get the GCP terra secret
   ```
   aws secretsmanager get-secret-value \
    --profile=embl-ebi \
    --region us-east-1 \
    --secret-id ingest/dev/secrets \
    --query SecretString \
    --output text | jq -jr '.ingest_exporter_terra_svc_account' > ~/.secrets/terra_secret
   ```
1. Set up ENV vars:
     ```
      INGEST_API=https://api.ingest.<ENV>.archive.data.humancellatlas.org/;
      RABBIT_URL=amqp://localhost:5672;
      GCP_SVC_ACCOUNT_KEY_PATH=~/.secrets/terra_secret;
      
      # These can be retrieved from the exporter section of dev/staging/prod.yaml in
      # https://github.com/ebi-ait/ingest-kube-deployment/blob/master/apps/
      TERRA_BUCKET_NAME=<FILL IN>;
      TERRA_BUCKET_PREFIX=<FILL IN>;
      GCP_PROJECT=<FILL IN>>;
   
      # These can be retrieved from aws-secrets
      # aws secretsmanager get-secret-value --secret-id ingest/<ENV>/secrets --profile=embl-ebi --query SecretString --output text | jq -jr '.ingest_exporter_access_key'
      AWS_ACCESS_KEY_ID=<FILL IN>;
      # aws secretsmanager get-secret-value --secret-id ingest/<ENV>/secrets --profile=embl-ebi --query SecretString --output text | jq -jr '.ingest_exporter_access_secret'
      AWS_ACCESS_KEY_SECRET=<FILL IN>
      ```
1. Once you're finished, re-deploy exporter via GitLab

## How to run tests
```bash
pip install -r requirements-dev.txt
nosetests
```

# Documentation
[Terra Staging Area Access](https://ebi-ait.github.io/hca-ebi-dev-team/admin_setup/Setting-up-access-to-Terra-staging-area.html)
[How to update GCP credentials of Ingest Exporter to access Terra staging area](https://ebi-ait.github.io/hca-ebi-dev-team/operations_tasks/update-exporter-gcp-creds.html)
[DCP2 exporter design](https://docs.google.com/document/d/15zxIHub2erKGWW7uGmep7ZjqsTNcI3gn5oDQmMB7Eic/edit#heading=h.4omhtn4kfq4x)