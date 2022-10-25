import os
from threading import Thread

from hca_ingest.api.ingestapi import IngestApi

from exporter.ingest.service import IngestService
from exporter.ingest.spreadsheet.handler import SpreadsheetHandler
from exporter.queue.config import QueueConfig, AmqpConnConfig
from exporter.queue.connector import QueueConnector
from exporter.queue.listener import QueueListener
from exporter.schema.service import SchemaService
from exporter.terra.experiment.client import TerraStorageClient
from exporter.terra.experiment.config import TerraConfig
from exporter.terra.gcs.config import GcpConfig
from exporter.terra.gcs.storage import GcsStorage
from exporter.terra.submission.client import TerraTransferClient

EXCHANGE = 'ingest.exporter.exchange'
SPREADSHEET_QUEUE_CONFIG = QueueConfig(
    EXCHANGE,
    routing_key='ingest.exporter.spreadsheet.requested',
    name='ingest.spreadsheets.new',
    queue_arguments={
        'x-dead-letter-exchange': 'ingest.exporter.exchange',
        'x-dead-letter-routing-key': 'ingest.terra.submission.error'
    }
)


def setup_spreadsheet_generator_exporter() -> Thread:
    rabbit_host = os.environ.get('RABBIT_HOST', 'localhost')
    rabbit_port = int(os.environ.get('RABBIT_PORT', '5672'))
    amqp_conn_config = AmqpConnConfig(rabbit_host, rabbit_port)

    ingest_api_url = os.environ.get('INGEST_API', 'localhost:8080')
    ingest_client = IngestApi(ingest_api_url)
    ingest_service = IngestService(ingest_client)
    schema_service = SchemaService(ingest_client)

    gcp_config = GcpConfig.from_env()
    gcs_storage = GcsStorage(gcp_config.gcp_project, gcp_config.gcs_svc_credentials_path)
    terra_config = TerraConfig.from_env()
    terra_client = TerraStorageClient(gcs_storage, schema_service, terra_config.terra_bucket_name, terra_config.terra_bucket_prefix)


    handler = SpreadsheetHandler(ingest_service, terra_client)
    listener = QueueListener(SPREADSHEET_QUEUE_CONFIG, handler)
    connector = QueueConnector(amqp_conn_config, listener)

    spreadsheet_listener_process = Thread(target=lambda: connector.run())
    spreadsheet_listener_process.start()
    return spreadsheet_listener_process