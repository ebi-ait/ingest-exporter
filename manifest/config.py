import os
from threading import Thread

from hca_ingest.api.ingestapi import IngestApi
from kombu import Connection

from exporter.graph.crawler import GraphCrawler
from exporter.metadata.service import MetadataService
from exporter.queue.config import QueueConfig
from manifest.exporter import ManifestExporter
from manifest.generator import ManifestGenerator
from manifest.receiver import ManifestReceiver

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


def setup_manifest_receiver() -> Thread:
    ingest_client = IngestApi()

    with Connection(DEFAULT_RABBIT_URL) as conn:
        manifest_generator = ManifestGenerator(ingest_client, GraphCrawler(MetadataService(ingest_client)))
        exporter = ManifestExporter(ingest_api=ingest_client, manifest_generator=manifest_generator)
        manifest_receiver = ManifestReceiver(conn, [ASSAY_QUEUE_CONFIG], exporter=exporter, publish_config=ASSAY_COMPLETE_CONFIG)
        manifest_process = Thread(target=manifest_receiver.run)
        manifest_process.start()

        return manifest_process
