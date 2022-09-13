from kombu import Message

from exporter.ingest.service import IngestService
from exporter.queue.config import QueueConfig
from exporter.queue.handler import MessageHandler
from exporter.session_context import SessionContext
from exporter.terra.experiment.exporter import TerraExperimentExporter
from exporter.terra.experiment.message import ExperimentMessage


class TerraExperimentHandler(MessageHandler):
    def __init__(
            self,
            experiment_exporter: TerraExperimentExporter,
            ingest_service: IngestService,
            publish_queue_config: QueueConfig
    ):
        super().__init__('TerraExperimentExporter')
        self.experiment_exporter = experiment_exporter
        self.ingest_service = ingest_service
        self.publish_queue = publish_queue_config

    def set_context(self, body: dict) -> SessionContext:
        return SessionContext(
            logger=self.logger,
            context={
                'submission_uuid': body.get('envelopeUuid'),
                'export_job_id': body.get('exportJobId'),
                'project_uuid': body.get('projectUuid')
            }
        )

    def handle_message(self, body: dict, msg: Message):
        exp = ExperimentMessage.from_dict(body)
        self.logger.info(f'Received experiment message for process {exp.process_uuid} (index {exp.experiment_index} for submission {exp.submission_uuid})')
        self.experiment_exporter.export(exp.process_uuid)
        self.logger.info(f'Exported experiment for process uuid {exp.process_uuid} (--index {exp.experiment_index} --total {exp.total} --submission {exp.submission_uuid})')
        self.log_complete_experiment(msg, exp)

    def log_complete_experiment(self, msg: Message, experiment: ExperimentMessage):
        self.logger.info(f'Marking successful experiment job_id {experiment.job_id} and process_id {experiment.process_id}')
        self.ingest_service.create_export_entity(experiment.job_id, experiment.process_id)
        self.logger.info(f'Creating new message in publish queue for experiment: {experiment}')
        self.publish_queue.send_message(self.producer, ExperimentMessage.as_dict(experiment))
        self.logger.info(f'Acknowledging export experiment message: {experiment}')
        msg.ack()
