import os
from threading import Thread
from typing import Tuple

from hca_ingest.api.ingestapi import IngestApi

from exporter.ingest.service import IngestService
from exporter.queue.config import QueueConfig, AmqpConnConfig
from exporter.queue.connector import QueueConnector
from exporter.queue.listener import QueueListener
from exporter.terra.gcs.config import GcpConfig
from exporter.terra.transfer import TerraTransferClient

from .exporter import TerraSubmissionExporter
from .handler import TerraSubmissionHandler
from .responder import TerraTransferResponder


LOGGER_NAME = 'TerraSubmissionExporter'
EXCHANGE = 'ingest.exporter.exchange'
SUBMISSION_QUEUE_CONFIG = QueueConfig(
    EXCHANGE,
    routing_key='ingest.exporter.submission.submitted',
    name='ingest.terra.submissions.new',
    queue_arguments={
        'x-dead-letter-exchange': 'ingest.exporter.exchange',
        'x-dead-letter-routing-key': 'ingest.terra.submission.error'
    }
)


def setup_terra_submissions_exporter() -> Tuple[Thread, Thread]:
    rabbit_host = os.environ.get('RABBIT_HOST', 'localhost')
    rabbit_port = int(os.environ.get('RABBIT_PORT', '5672'))
    amqp_conn_config = AmqpConnConfig(rabbit_host, rabbit_port)

    ingest_api_url = os.environ.get('INGEST_API', 'localhost:8080')

    ingest_client = IngestApi(ingest_api_url)
    ingest_service = IngestService(ingest_client)
    terra_client = TerraTransferClient.from_env()
    terra_exporter = TerraSubmissionExporter(ingest_service, terra_client, LOGGER_NAME)

    handler = TerraSubmissionHandler(terra_exporter, ingest_service, LOGGER_NAME)
    listener = QueueListener(SUBMISSION_QUEUE_CONFIG, handler)
    connector = QueueConnector(amqp_conn_config, listener)

    terra_exporter_listener_process = Thread(target=lambda: connector.run())
    terra_exporter_listener_process.start()
    gcp_config = GcpConfig.from_env()
    terra_responder = TerraTransferResponder(ingest_service, gcp_config)
    terra_transfer_complete_listener = Thread(target=lambda: terra_responder.listen())
    terra_transfer_complete_listener.start()

    return terra_exporter_listener_process, terra_transfer_complete_listener


