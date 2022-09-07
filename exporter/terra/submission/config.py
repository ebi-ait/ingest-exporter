import os
from threading import Thread
from typing import Tuple

from hca_ingest.api.ingestapi import IngestApi

from exporter.ingest.service import IngestService
from exporter.queue.config import QueueConfig, AmqpConnConfig
from exporter.queue.connector import QueueConnector
from exporter.queue.listener import QueueListener
from exporter.terra.gcs.transfer import GcsTransfer
from exporter.terra.submission.client import TerraTransferClient
from exporter.terra.submission.exporter import TerraSubmissionExporter
from exporter.terra.submission.handler import TerraSubmissionHandler
from exporter.terra.submission.responder import TerraTransferResponder

EXCHANGE = 'ingest.exporter.exchange'
RETRY_POLICY = {
    'interval_start': 0,
    'interval_step': 2,
    'interval_max': 30,
    'max_retries': 60
}
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
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_access_key_secret = os.environ['AWS_ACCESS_KEY_SECRET']
    gcp_credentials_path = os.environ['GCP_SVC_ACCOUNT_KEY_PATH']
    gcp_project = os.environ['GCP_PROJECT']
    gcp_topic = os.environ['GCP_TRANSFER_TOPIC']

    terra_bucket_name = os.environ['TERRA_BUCKET_NAME']
    terra_bucket_prefix = os.environ['TERRA_BUCKET_PREFIX']

    ingest_client = IngestApi(ingest_api_url)
    ingest_service = IngestService(ingest_client)
    gcs_transfer = GcsTransfer(gcp_credentials_path)
    terra_client = TerraTransferClient(gcs_transfer, aws_access_key_id, aws_access_key_secret, gcp_project, terra_bucket_name, terra_bucket_prefix, gcp_topic)
    terra_exporter = TerraSubmissionExporter(ingest_service, terra_client)

    handler = TerraSubmissionHandler(terra_exporter, ingest_service)
    listener = QueueListener(SUBMISSION_QUEUE_CONFIG, handler)
    connector = QueueConnector(amqp_conn_config, listener)

    terra_exporter_listener_process = Thread(target=lambda: connector.run())
    terra_exporter_listener_process.start()

    terra_responder = TerraTransferResponder(ingest_service, gcp_project, gcp_topic, gcp_credentials_path)
    terra_transfer_complete_listener = Thread(target=lambda: terra_responder.listen())
    terra_transfer_complete_listener.start()

    return terra_exporter_listener_process, terra_transfer_complete_listener
