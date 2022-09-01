import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Type, List

from kombu import Connection, Consumer, Message
from kombu.mixins import ConsumerProducerMixin

from exporter.ingest.service import IngestService
from exporter.queue.config import QueueConfig
from exporter.session_context import SessionContext
from exporter.terra.experiment.exporter import TerraExperimentExporter
from exporter.terra.experiment.message import ExperimentMessage


class TerraExperimentListener(ConsumerProducerMixin):
    def __init__(self,
                 connection: Connection,
                 terra_exporter: TerraExperimentExporter,
                 ingest_service: IngestService,
                 experiment_queue_config: QueueConfig,
                 publish_queue_config: QueueConfig,
                 executor: ThreadPoolExecutor):
        self.connection = connection
        self.terra_exporter = terra_exporter
        self.ingest_service = ingest_service
        self.experiment_queue = experiment_queue_config
        self.publish_queue = publish_queue_config
        self.executor = executor

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def get_consumers(self, _consumer: Type[Consumer], channel) -> List[Consumer]:
        experiment_consumer = _consumer([self.experiment_queue.queue_from_config()],
                                        callbacks=[self.experiment_message_handler],
                                        prefetch_count=1)

        return [experiment_consumer]

    def experiment_message_handler(self, body: str, msg: Message):
        return self.executor.submit(lambda: self._experiment_message_handler(body, msg))

    def _experiment_message_handler(self, body: str, msg: Message):
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
                    self.logger.info(f'Received experiment message for process {exp.process_uuid} (index {exp.experiment_index} for submission {submission_uuid})')
                    self.terra_exporter.export(exp.process_uuid, submission_uuid, export_job_id)
                    self.logger.info(f'Exported experiment for process uuid {exp.process_uuid} (--index {exp.experiment_index} --total {exp.total} --submission {submission_uuid})')
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
        self.logger.info(f'Marking successful experiment job_id {experiment.job_id} and process_id {experiment.process_id}')
        self.ingest_service.create_export_entity(experiment.job_id, experiment.process_id)
        self.logger.info(f'Creating new message in publish queue for experiment: {experiment}')
        self.publish_queue.send_message(self.producer, ExperimentMessage.as_dict(experiment))
        self.logger.info(f'Acknowledging export experiment message: {experiment}')
        msg.ack()
