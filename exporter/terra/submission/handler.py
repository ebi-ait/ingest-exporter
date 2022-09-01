from kombu import Message

from exporter.ingest.service import IngestService
from exporter.queue.handler import MessageHandler
from exporter.session_context import SessionContext
from exporter.terra.submission.exporter import TerraSubmissionExporter
from exporter.terra.submission.message import SubmissionMessage


class TerraSubmissionHandler(MessageHandler):
    def __init__(self, submission_exporter: TerraSubmissionExporter, ingest_service: IngestService):
        super().__init__()
        self.submission_exporter = submission_exporter
        self.ingest_service = ingest_service

    def handle_message(self, body: str, msg: Message):
        export = SubmissionMessage(body)
        with SessionContext(
                logger=self.logger,
                context={
                    'submission_uuid': export.submission_uuid,
                    'export_job_id': export.job_id,
                }
        ):
            self.logger.info(f'Received export submission message for project {export.project_uuid}')
            self.submission_exporter.start_data_file_transfer(export.job_id, export.submission_uuid, export.project_uuid)
            self.logger.info(f'Started data sync for project: {export.project_uuid}')
            self.ingest_service.set_data_file_transfer(export.job_id, "STARTED")
            self.logger.info(f'Acknowledging export submission for project: {export.project_uuid}')
            msg.ack()
