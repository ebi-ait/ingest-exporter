from kombu import Message

from exporter.ingest.service import IngestService
from exporter.queue.config import QueueConfig
from exporter.queue.handler import MessageHandler
from exporter.session_context import SessionContext

from .exporter import TerraExperimentExporter
from .message import ExperimentMessage


class TerraExperimentHandler(MessageHandler):
    def __init__(
            self,
            experiment_exporter: TerraExperimentExporter,
            ingest_service: IngestService,
            publish_queue_config: QueueConfig,
            logger_name: str = __name__
    ):
        super().__init__(logger_name)
        self.experiment_exporter = experiment_exporter
        self.ingest_service = ingest_service
        self.publish_queue = publish_queue_config

    def set_context(self, body: dict) -> SessionContext:
        return SessionContext(
            logger=self.logger,
            context={
                'submission_uuid': body.get('envelopeUuid'),
                'export_job_id': body.get('exportJobId'),
                'project_uuid': body.get('projectUuid'),
                'process_uuid': body.get('documentUuid'),
                'index': f'{body.get("index")}/{body.get("total")}'
            }
        )

    def handle_message(self, body: dict, msg: Message):
        exp = ExperimentMessage.from_dict(body)
        self.logger.info(f'Received experiment export message.')
        self.experiment_exporter.export(exp.process_uuid)
        self.logger.info('Experiment export finished, informing ingest')
        self.ingest_service.create_export_entity(exp.job_id, exp.process_id)
        self.publish_queue.send_message(self.producer, body)
        self.logger.info(f'Acknowledging experiment export message')
        msg.ack()
