import logging
import os
import uuid

from hca_ingest.api.ingestapi import IngestApi
from requests import HTTPError

from exporter.ingest.service import IngestService
from exporter.schema.service import SchemaService
from exporter.session_context import SessionContext
from exporter.terra.config import TerraConfig
from exporter.terra.gcs.config import GcpConfig
from exporter.terra.gcs.storage import GcsStorage
from exporter.terra.spreadsheet.exporter import SpreadsheetExporter
from exporter.terra.storage import TerraStorageClient


class SpreadsheetReExporter(SpreadsheetExporter):
    def delete_old_files(self, project_uuid: str):
        old_filename = f'metadata_{project_uuid}.xlsx'
        self.terra.delete_data_file(project_uuid, old_filename)
        metadata_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f'{old_filename}_metadata'))
        self.terra.delete_metadata(project_uuid, 'file', 'supplementary_file', metadata_uuid)


logging.basicConfig(level=logging.CRITICAL)
LOGGER_NAME = "ReExportSpreadsheets"
logger = SessionContext.register_logger(LOGGER_NAME)
api_url = os.environ['INGEST_API']
ingest_api = IngestApi(api_url)
ingest_service = IngestService(ingest_api)
schema_service = SchemaService(ingest_api)

submission_project_uuids = [
    ('submission_uuid_1', 'project_uuid_1'),
    ('submission_uuid_2', 'project_uuid_2')
]

gcp_config = GcpConfig.from_env()
gcs_storage = GcsStorage(gcp_config.gcp_project, gcp_config.gcp_credentials_path, LOGGER_NAME)
terra_config = TerraConfig.from_env()
terra_client = TerraStorageClient(gcs_storage, schema_service, terra_config.terra_bucket_name, terra_config.terra_bucket_prefix, LOGGER_NAME)

exporter = SpreadsheetReExporter(ingest_service, terra_client, LOGGER_NAME)

for submission, project in submission_project_uuids:
    try:
        exporter.delete_old_files(project)
        exporter.export_spreadsheet(project, submission)
    except HTTPError as e:
        logger.error(f'Error: reexporting {project}, {str(e)}')
