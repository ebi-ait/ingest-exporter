import json
import logging

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
        super().__init__()
        self.experiment_exporter = experiment_exporter
        self.ingest_service = ingest_service
        self.publish_queue = publish_queue_config

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def handle_message(self, body: str, msg: Message):
        self.logger.info(f'Message received: {body}')
        try:
            exp = ExperimentMessage.from_dict(json.loads(body))
            submission_uuid = exp.submission_uuid
            export_job_id = exp.job_id
            with SessionContext(logger=self.logger,
                                context={
                                    'submission_uuid': submission_uuid,
                                    'export_job_id': export_job_id,
                                }):
                try:
                    self.logger.info(
                        f'Received experiment message for process {exp.process_uuid} (index {exp.experiment_index} for submission {submission_uuid})')
                    self.experiment_exporter.export(exp.process_uuid, submission_uuid, export_job_id)
                    self.logger.info(
                        f'Exported experiment for process uuid {exp.process_uuid} (--index {exp.experiment_index} --total {exp.total} --submission {submission_uuid})')
                    self.log_complete_experiment(msg, exp)
                except Exception as e:
                    self.logger.error(f'Rejecting export experiment: {exp} due to error: {str(e)}')
                    msg.reject(requeue=False)
                    self.logger.exception(e)

        except Exception as e:
            self.logger.error(f"Rejecting export experiment message: {body} due to error: {str(e)}")
            msg.reject(requeue=False)
            self.logger.exception(e)

    def log_complete_experiment(self, msg: Message, experiment: ExperimentMessage):
        self.logger.info(
            f'Marking successful experiment job_id {experiment.job_id} and process_id {experiment.process_id}')
        self.ingest_service.create_export_entity(experiment.job_id, experiment.process_id)
        self.logger.info(f'Creating new message in publish queue for experiment: {experiment}')
        self.publish_queue.send_message(self.producer, ExperimentMessage.as_dict(experiment))
        self.logger.info(f'Acknowledging export experiment message: {experiment}')
        msg.ack()