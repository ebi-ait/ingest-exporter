import json

from google.cloud import storage
from google.oauth2.service_account import Credentials
from hca_ingest.api.ingestapi import IngestApi

from exporter.schema.service import SchemaService
from exporter.terra.client import TerraClient
from exporter.terra.gcs.storage import GcsStorage
from exporter.terra.gcs.transfer import GcsTransfer


class ClientBuilder:
    def __init__(self):
        self.ingest_client = None
        self.schema_service = None
        self.gcs_storage = None
        self.gcs_xfer = None

    def with_gcs_info(
            self,
            service_account_credentials_path: str,
            gcp_project: str,
            bucket_name: str,
            bucket_prefix: str
    ) -> 'ClientBuilder':
        with open(service_account_credentials_path) as source:
            info = json.load(source)
            storage_credentials: Credentials = Credentials.from_service_account_info(info)
            gcs_client = storage.Client(project=gcp_project, credentials=storage_credentials)
            self.gcs_storage = GcsStorage(gcs_client, bucket_name, bucket_prefix)
            return self

    def with_gcs_xfer(
            self,
            service_account_credentials_path: str,
            gcp_project: str,
            bucket_name: str,
            bucket_prefix: str,
            aws_access_key_id: str,
            aws_access_key_secret: str
    ) -> 'ClientBuilder':
        with open(service_account_credentials_path) as source:
            info = json.load(source)
            credentials: Credentials = Credentials.from_service_account_info(info)
            self.gcs_xfer = GcsTransfer(aws_access_key_id, aws_access_key_secret, gcp_project,
                                        bucket_name, bucket_prefix, credentials)
            return self

    def with_ingest_client(self, ingest_client: IngestApi) -> 'ClientBuilder':
        self.ingest_client = ingest_client
        return self

    def with_schema_service(self, schema_service: SchemaService) -> 'ClientBuilder':
        self.schema_service = schema_service
        return self

    def build(self) -> TerraClient:
        if not self.gcs_xfer:
            raise Exception("gcs_xfer must be set")
        elif not self.gcs_storage:
            raise Exception("gcs_storage must be set")
        elif not self.schema_service:
            raise Exception("schema_service must be set")
        elif not self.ingest_client:
            raise Exception("ingest_client must be set")
        return TerraClient(self.gcs_storage, self.gcs_xfer, self.schema_service, self.ingest_client)
