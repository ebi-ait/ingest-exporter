import logging
import os

from hca_ingest.api.ingestapi import IngestApi

from exporter.ingest.service import IngestService
from exporter.schema.service import SchemaService
from exporter.session_context import SessionContext
from exporter.terra.config import TerraConfig
from exporter.terra.gcs.config import GcpConfig
from exporter.terra.gcs.storage import GcsStorage
from exporter.terra.spreadsheet.exporter import SpreadsheetExporter
from exporter.terra.storage import TerraStorageClient

logging.basicConfig(level=logging.CRITICAL)
LOGGER_NAME = __name__
logger = SessionContext.register_logger(LOGGER_NAME)
api_url = os.environ['INGEST_API']
ingest_api = IngestApi(api_url)
ingest_service = IngestService(ingest_api)
schema_service = SchemaService(ingest_api)

submission_project_uuids = [
    # ('submission', 'project'),
    # ('51945966-78ff-4d20-890f-457cf986a2d3', '12f32054-8f18-4dae-8959-bfce7e3108e7'),
    # ('baf35d2a-5afd-4eca-a38c-6ea8bbefb9ec', 'cd9d6360-ce38-4321-97df-f13c79e3cb84'),
    # ('0948a727-228f-4cfc-857e-6243c6aed08d', '2043c65a-1cf8-4828-a656-9e247d4e64f1'),
    # ('5f7fa8b1-f35b-4a2d-b385-c2a686acbc9c', '3ce9ae94-c469-419a-9637-5d138a4e642f'),
    # ('d78adee7-e3d1-46bc-bca4-7822181bd8af', 'cbd2911f-252b-4428-abde-69e270aefdfc'),
    # ('670faf2b-cfb8-411a-8400-91168cdd139c', '957261f7-2bd6-4358-a6ed-24ee080d5cfc'),
    # ('639d3383-47a6-4436-985e-4eedd8ee46bb', '5f607e50-ba22-4598-b1e9-f3d9d7a35dcc'),
    # ('fcf7da86-018e-4cc1-a2f1-7a23648223b7', '6e60a555-fd95-4aa2-8e29-3ec2ef01a580'),
    # ('85b5d566-bb78-4bed-89e6-2f44e7b6bd48', '5b328561-4a97-40ac-b7ad-6a90fc59d374'),
    # ('fbd97e6f-625d-4e61-8cc9-50f2c82f2c5a', 'e88714c2-2e78-49da-8146-5a60b50628b4'),
    # ('fc4feb0b-3749-49b5-b15b-0a9dc1dd6e2f', 'e57dc176-ab98-446b-90c2-89e0842152fd'),
    # ('3da6718d-aeb4-4376-acd3-6ee24994e2a0', '77dedd59-1376-4887-9bca-dc42b56d5b7a'),
]

gcp_config = GcpConfig.from_env()
gcs_storage = GcsStorage(gcp_config.gcp_project, gcp_config.gcp_credentials_path, LOGGER_NAME)
terra_config = TerraConfig.from_env()
terra_client = TerraStorageClient(gcs_storage, schema_service, terra_config.terra_bucket_name,
                                  terra_config.terra_bucket_prefix, LOGGER_NAME)

exporter = SpreadsheetExporter(ingest_service, terra_client, LOGGER_NAME)

for submission, project in submission_project_uuids:
    try:
        logger.info(f'generating spreadsheet for project: {project}, submission: {submission}')
        exporter.export_spreadsheet(project, submission)
        logger.info(f'finished project: {project}, submission: {submission}')
    except Exception as e:
        logger.error(f'Error: reexporting {project}, {str(e)}', e)
