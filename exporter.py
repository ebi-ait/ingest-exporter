#!/usr/bin/env python
import logging
import os
from threading import Thread

from ingest.api.ingestapi import IngestApi
from kombu import Connection, Exchange, Queue

from exporter.amqp import AmqpConnConfig, QueueConfig
from exporter.graph.graph_crawler import GraphCrawler
from exporter.metadata import MetadataService
from exporter.schema import SchemaService
from exporter.session_context import configure_logger
from exporter.terra.dcp_staging_client import DcpStagingClient
from exporter.terra.terra_export_job import TerraExportJobService
from exporter.terra.terra_exporter import TerraExporter
from exporter.terra.terra_listener import TerraListener
from manifest.exporter import ManifestExporter
from manifest.generator import ManifestGenerator
from manifest.receiver import ManifestReceiver

DISABLE_MANIFEST = os.environ.get('DISABLE_MANIFEST', False)

DEFAULT_RABBIT_URL = os.path.expandvars(
    os.environ.get('RABBIT_URL', 'amqp://localhost:5672'))

EXCHANGE = 'ingest.exporter.exchange'
EXCHANGE_TYPE = 'topic'

ASSAY_QUEUE_MANIFEST = 'ingest.manifests.assays.new'
EXPERIMENT_QUEUE_TERRA = 'ingest.terra.experiments.new'

ASSAY_ROUTING_KEY = 'ingest.exporter.manifest.submitted'
EXPERIMENT_ROUTING_KEY = 'ingest.exporter.experiment.submitted'

ASSAY_COMPLETED_ROUTING_KEY = 'ingest.exporter.manifest.completed'
EXPERIMENT_COMPLETED_ROUTING_KEY = 'ingest.exporter.experiment.exported'


RETRY_POLICY = {
    'interval_start': 0,
    'interval_step': 2,
    'interval_max': 30,
    'max_retries': 60
}


def setup_manifest_receiver() -> Thread:
    ingest_client = IngestApi()

    with Connection(DEFAULT_RABBIT_URL) as conn:
        bundle_exchange = Exchange(EXCHANGE, type=EXCHANGE_TYPE)
        bundle_queues = [
            Queue(ASSAY_QUEUE_MANIFEST, bundle_exchange,
                  routing_key=ASSAY_ROUTING_KEY)
        ]

        conf = {
            'exchange': EXCHANGE,
            'routing_key': ASSAY_COMPLETED_ROUTING_KEY,
            'retry': True,
            'retry_policy': RETRY_POLICY
        }

        manifest_generator = ManifestGenerator(ingest_client, GraphCrawler(MetadataService(ingest_client)))
        exporter = ManifestExporter(ingest_api=ingest_client, manifest_generator=manifest_generator)
        manifest_receiver = ManifestReceiver(conn, bundle_queues, exporter=exporter, publish_config=conf)
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
    dcp_staging_client = (DcpStagingClient
                          .Builder()
                          .with_ingest_client(ingest_client)
                          .with_schema_service(schema_service)
                          .with_gcs_info(gcs_svc_credentials_path, gcp_project, terra_bucket_name, terra_bucket_prefix)
                          .with_gcs_xfer(gcs_svc_credentials_path, gcp_project, terra_bucket_name, terra_bucket_prefix, aws_access_key_id, aws_access_key_secret)
                          .build())

    terra_job_service = TerraExportJobService(ingest_client)
    terra_exporter = TerraExporter(ingest_client, metadata_service, graph_crawler, dcp_staging_client, terra_job_service)

    rabbit_host = os.environ.get('RABBIT_HOST', 'localhost')
    rabbit_port = int(os.environ.get('RABBIT_PORT', '5672'))
    amqp_conn_config = AmqpConnConfig(rabbit_host, rabbit_port)
    experiment_queue_config = QueueConfig(EXPERIMENT_QUEUE_TERRA, EXPERIMENT_ROUTING_KEY, EXCHANGE, EXCHANGE_TYPE, False, None)
    publish_queue_config = QueueConfig(None, EXPERIMENT_COMPLETED_ROUTING_KEY, EXCHANGE, EXCHANGE_TYPE, True, RETRY_POLICY)

    terra_listener = TerraListener(amqp_conn_config, terra_exporter, terra_job_service, experiment_queue_config, publish_queue_config)

    terra_exporter_listener_process = Thread(target=lambda: terra_listener.run())
    terra_exporter_listener_process.start()

    return terra_exporter_listener_process


if __name__ == '__main__':
    inegst_logger = logging.getLogger('ingest')
    inegst_logger.setLevel(logging.INFO)
    configure_logger(inegst_logger)
    manifest_logger = logging.getLogger('manifest')
    manifest_logger.setLevel(logging.INFO)
    configure_logger(manifest_logger)

    manifest_thread = None
    if not DISABLE_MANIFEST:
        manifest_thread = setup_manifest_receiver()

    terra_thread = setup_terra_exporter()

    if not DISABLE_MANIFEST:
        manifest_thread.join()
    terra_thread.join()

