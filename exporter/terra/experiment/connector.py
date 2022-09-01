from concurrent.futures import ThreadPoolExecutor

from kombu import Connection

from exporter.ingest.service import IngestService
from exporter.queue.config import AmqpConnConfig, QueueConfig
from exporter.terra.experiment.exporter import TerraExperimentExporter
from exporter.terra.experiment.listener import TerraExperimentListener


class ExperimentConnector:
    def __init__(self,
                 amqp_conn_config: AmqpConnConfig,
                 terra_exporter: TerraExperimentExporter,
                 ingest_service: IngestService,
                 experiment_queue_config: QueueConfig,
                 publish_queue_config: QueueConfig):
        self.amqp_conn_config = amqp_conn_config
        self.terra_exporter = terra_exporter
        self.ingest_service = ingest_service
        self.experiment_queue_config = experiment_queue_config
        self.publish_queue_config = publish_queue_config

    def run(self):
        with Connection(self.amqp_conn_config.broker_url()) as conn:
            _terra_listener = TerraExperimentListener(conn, self.terra_exporter, self.ingest_service, self.experiment_queue_config, self.publish_queue_config, ThreadPoolExecutor())
            _terra_listener.run()
