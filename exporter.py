#!/usr/bin/env python
import logging
import os
from threading import Thread

from hca_ingest.api.ingestapi import IngestApi
from kombu import Connection

from exporter.graph.graph_crawler import GraphCrawler
from exporter.ingest.service import IngestService
from exporter.metadata.service import MetadataService
from exporter.queue.config import AmqpConnConfig, QueueConfig
from exporter.schema import SchemaService
from exporter.session_context import configure_logger
from exporter.terra.client import TerraClient
from exporter.terra.exporter import TerraExporter
from exporter.terra.listener import TerraListener
from manifest.exporter import ManifestExporter
from manifest.generator import ManifestGenerator
from manifest.receiver import ManifestReceiver

DISABLE_MANIFEST = os.environ.get('DISABLE_MANIFEST', False)

DEFAULT_RABBIT_URL = os.path.expandvars(
    os.environ.get('RABBIT_URL', 'amqp://localhost:5672'))

EXCHANGE = 'ingest.exporter.exchange'

RETRY_POLICY = {
    'interval_start': 0,
    'interval_step': 2,
    'interval_max': 30,
    'max_retries': 60
}
ASSAY_QUEUE_CONFIG = QueueConfig(
    EXCHANGE,
    routing_key='ingest.exporter.manifest.submitted',
    name='ingest.manifests.assays.new',
    queue_arguments={
        'x-dead-letter-exchange': 'ingest.exporter.exchange',
        'x-dead-letter-routing-key': 'ingest.manifest.assay.error'
    }
)
ASSAY_COMPLETE_CONFIG = QueueConfig(
    EXCHANGE,
    routing_key='ingest.exporter.manifest.completed',
    retry=True,
    retry_policy=RETRY_POLICY
)
EXPERIMENT_QUEUE_CONFIG = QueueConfig(
    EXCHANGE,
    routing_key='ingest.exporter.experiment.submitted',
    name='ingest.terra.experiments.new',
    queue_arguments={
        'x-dead-letter-exchange': 'ingest.exporter.exchange',
        'x-dead-letter-routing-key': 'ingest.terra.experiment.error'
    }
)
EXPERIMENT_COMPLETE_CONFIG = QueueConfig(
    EXCHANGE,
    routing_key='ingest.exporter.experiment.exported',
    retry=True,
    retry_policy=RETRY_POLICY
)


def setup_manifest_receiver() -> Thread:
    ingest_client = IngestApi()

    with Connection(DEFAULT_RABBIT_URL) as conn:
        manifest_generator = ManifestGenerator(ingest_client, GraphCrawler(MetadataService(ingest_client)))
        exporter = ManifestExporter(ingest_api=ingest_client, manifest_generator=manifest_generator)
        manifest_receiver = ManifestReceiver(conn, [ASSAY_QUEUE_CONFIG], exporter=exporter, publish_config=ASSAY_COMPLETE_CONFIG)
        manifest_process = Thread(target=manifest_receiver.run)
        manifest_process.start()

        return manifest_process


def setup_terra_exporter() -> Thread:
    ingest_api_url = os.environ.get('INGEST_API', 'localhost:8080')
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_access_key_secret = os.environ['AWS_ACCESS_KEY_SECRET']
    gcs_svc_credentials_path = os.environ['GCP_SVC_ACCOUNT_KEY_PATH']
    gcp_project = os.environ['GCP_PROJECT']
    terra_bucket_name = os.environ['TERRA_BUCKET_NAME']
    terra_bucket_prefix = os.environ['TERRA_BUCKET_PREFIX']

    ingest_client = IngestApi(ingest_api_url)

    metadata_service = MetadataService(ingest_client)
    schema_service = SchemaService(ingest_client)
    graph_crawler = GraphCrawler(metadata_service)
    dcp_staging_client = (TerraClient
                          .Builder()
                          .with_ingest_client(ingest_client)
                          .with_schema_service(schema_service)
                          .with_gcs_info(gcs_svc_credentials_path, gcp_project, terra_bucket_name, terra_bucket_prefix)
                          .with_gcs_xfer(gcs_svc_credentials_path, gcp_project, terra_bucket_name, terra_bucket_prefix, aws_access_key_id, aws_access_key_secret)
                          .build())

    ingest_service = IngestService(ingest_client)
    terra_exporter = TerraExporter(ingest_client, metadata_service, graph_crawler, dcp_staging_client, ingest_service)

    rabbit_host = os.environ.get('RABBIT_HOST', 'localhost')
    rabbit_port = int(os.environ.get('RABBIT_PORT', '5672'))
    amqp_conn_config = AmqpConnConfig(rabbit_host, rabbit_port)

    terra_listener = TerraListener(amqp_conn_config, terra_exporter, ingest_service, EXPERIMENT_QUEUE_CONFIG, EXPERIMENT_COMPLETE_CONFIG)

    terra_exporter_listener_process = Thread(target=lambda: terra_listener.run())
    terra_exporter_listener_process.start()

    return terra_exporter_listener_process


if __name__ == '__main__':
    configure_logger(logging.getLogger(''))
    ingest_logger = logging.getLogger('ingest')
    ingest_logger.setLevel(logging.INFO)
    manifest_logger = logging.getLogger('manifest')
    manifest_logger.setLevel(logging.INFO)

    manifest_thread = None
    if not DISABLE_MANIFEST:
        manifest_thread = setup_manifest_receiver()

    terra_thread = setup_terra_exporter()

    if not DISABLE_MANIFEST:
        manifest_thread.join()
    terra_thread.join()

