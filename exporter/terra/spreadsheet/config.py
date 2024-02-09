import os
from threading import Thread

from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.utils.token_manager import TokenManager

from exporter.ingest.service import IngestService
from exporter.queue.config import QueueConfig, AmqpConnConfig
from exporter.queue.connector import QueueConnector
from exporter.queue.listener import QueueListener
from exporter.schema.service import SchemaService
from exporter.terra.storage import TerraStorageClient
from exporter.terra.config import TerraConfig
from exporter.terra.gcs.config import GcpConfig
from exporter.terra.gcs.storage import GcsStorage

from .handler import SpreadsheetHandler
from ...session_context import SessionContext
from ...utils import init_token_manager

LOGGER_NAME = 'TerraSpreadsheetExporter'
EXCHANGE = 'ingest.exporter.exchange'
SPREADSHEET_QUEUE_CONFIG = QueueConfig(
    EXCHANGE,
    routing_key='ingest.exporter.spreadsheet.requested',
    name='ingest.terra.spreadsheets.new',
    queue_arguments={
        'x-dead-letter-exchange': 'ingest.exporter.exchange',
        'x-dead-letter-routing-key': 'ingest.terra.spreadsheet.error'
    }
)


def setup_terra_spreadsheet_exporter() -> Thread:
    rabbit_host = os.environ.get('RABBIT_HOST', 'localhost')
    rabbit_port = int(os.environ.get('RABBIT_PORT', '5672'))
    amqp_conn_config = AmqpConnConfig(rabbit_host, rabbit_port)

    token_manager:TokenManager = init_token_manager()
    ingest_api_url = os.environ.get('INGEST_API', 'localhost:8080')
    ingest_client = IngestApi(url=ingest_api_url, token_manager=token_manager)

    ingest_service = IngestService(ingest_client)
    schema_service = SchemaService(ingest_client)

    gcp_config = GcpConfig.from_env()
    gcs_storage = GcsStorage(gcp_config.gcp_project, gcp_config.gcp_credentials_path, LOGGER_NAME)
    terra_config = TerraConfig.from_env()
    terra_client = TerraStorageClient(gcs_storage, schema_service, terra_config.terra_bucket_name, terra_config.terra_bucket_prefix, LOGGER_NAME)

    handler = SpreadsheetHandler(ingest_service, terra_client, LOGGER_NAME)
    listener = QueueListener(SPREADSHEET_QUEUE_CONFIG, handler)
    connector = QueueConnector(amqp_conn_config, listener)

    spreadsheet_listener_process = Thread(target=lambda: connector.run())
    spreadsheet_listener_process.start()
    return spreadsheet_listener_process
