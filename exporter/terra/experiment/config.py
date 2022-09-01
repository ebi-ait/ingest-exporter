import os
from threading import Thread

from hca_ingest.api.ingestapi import IngestApi

from exporter.graph.crawler import GraphCrawler
from exporter.ingest.service import IngestService
from exporter.metadata.service import MetadataService
from exporter.queue.config import QueueConfig, AmqpConnConfig
from exporter.schema.service import SchemaService
from exporter.terra.builder import ClientBuilder
from exporter.terra.experiment.connector import ExperimentConnector
from exporter.terra.experiment.exporter import TerraExperimentExporter

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
    terra_client = (ClientBuilder()
                          .with_ingest_client(ingest_client)
                          .with_schema_service(schema_service)
                          .with_gcs_info(gcs_svc_credentials_path, gcp_project, terra_bucket_name, terra_bucket_prefix)
                          .with_gcs_xfer(gcs_svc_credentials_path, gcp_project, terra_bucket_name, terra_bucket_prefix, aws_access_key_id, aws_access_key_secret)
                          .build())

    ingest_service = IngestService(ingest_client)
    terra_exporter = TerraExperimentExporter(ingest_client, metadata_service, graph_crawler, terra_client, ingest_service)

    rabbit_host = os.environ.get('RABBIT_HOST', 'localhost')
    rabbit_port = int(os.environ.get('RABBIT_PORT', '5672'))
    amqp_conn_config = AmqpConnConfig(rabbit_host, rabbit_port)

    terra_listener = ExperimentConnector(amqp_conn_config, terra_exporter, ingest_service, EXPERIMENT_QUEUE_CONFIG, EXPERIMENT_COMPLETE_CONFIG)

    terra_exporter_listener_process = Thread(target=lambda: terra_listener.run())
    terra_exporter_listener_process.start()

    return terra_exporter_listener_process
