import os
from threading import Thread

from hca_ingest.api.ingestapi import IngestApi

from exporter.graph.crawler import GraphCrawler
from exporter.ingest.service import IngestService
from exporter.metadata.service import MetadataService
from exporter.queue.config import QueueConfig, AmqpConnConfig
from exporter.queue.connector import QueueConnector
from exporter.queue.listener import QueueListener
from exporter.schema.service import SchemaService
from exporter.terra.experiment.client import storage_client_from_gcs_info, TerraStorageClient
from exporter.terra.experiment.exporter import TerraExperimentExporter
from exporter.terra.experiment.handler import TerraExperimentHandler

EXCHANGE = 'ingest.exporter.exchange'
RETRY_POLICY = {
    'interval_start': 0,
    'interval_step': 2,
    'interval_max': 30,
    'max_retries': 60
}
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


def setup_terra_experiment_exporter() -> Thread:
    rabbit_host = os.environ.get('RABBIT_HOST', 'localhost')
    rabbit_port = int(os.environ.get('RABBIT_PORT', '5672'))
    amqp_conn_config = AmqpConnConfig(rabbit_host, rabbit_port)

    ingest_api_url = os.environ.get('INGEST_API', 'localhost:8080')
    gcs_svc_credentials_path = os.environ['GCP_SVC_ACCOUNT_KEY_PATH']
    gcp_project = os.environ['GCP_PROJECT']
    terra_bucket_name = os.environ['TERRA_BUCKET_NAME']
    terra_bucket_prefix = os.environ['TERRA_BUCKET_PREFIX']

    ingest_client = IngestApi(ingest_api_url)

    metadata_service = MetadataService(ingest_client)
    schema_service = SchemaService(ingest_client)
    graph_crawler = GraphCrawler(metadata_service)
    gcs_storage = storage_client_from_gcs_info(gcs_svc_credentials_path, gcp_project, terra_bucket_name, terra_bucket_prefix)
    terra_client = TerraStorageClient(gcs_storage, schema_service)
    ingest_service = IngestService(ingest_client)
    terra_exporter = TerraExperimentExporter(ingest_service, graph_crawler, terra_client)

    handler = TerraExperimentHandler(terra_exporter, ingest_service, EXPERIMENT_COMPLETE_CONFIG)
    listener = QueueListener(EXPERIMENT_QUEUE_CONFIG, handler)
    connector = QueueConnector(amqp_conn_config, listener)

    terra_exporter_listener_process = Thread(target=lambda: connector.run())
    terra_exporter_listener_process.start()

    return terra_exporter_listener_process
