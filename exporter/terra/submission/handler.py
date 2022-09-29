from kombu import Message

from exporter.ingest.export_job import DataTransferState
from exporter.ingest.service import IngestService
from exporter.queue.handler import MessageHandler
from exporter.session_context import SessionContext
from exporter.terra.submission.exporter import TerraSubmissionExporter
from exporter.terra.submission.message import SubmissionExportMessage


class TerraSubmissionHandler(MessageHandler):
    def __init__(self, submission_exporter: TerraSubmissionExporter, ingest_service: IngestService):
        super().__init__('TerraSubmissionExporter')
        self.submission_exporter = submission_exporter
        self.ingest_service = ingest_service

    def set_context(self, body: dict) -> SessionContext:
        return SessionContext(
            logger=self.logger,
            context={
                'submission_uuid': body.get('submissionUuid'),
                'export_job_id': body.get('exportJobId'),
                'project_uuid': body.get('projectUuid')
            }
        )

    def handle_message(self, body: dict, msg: Message):
        export = SubmissionExportMessage(body)
        self.logger.info('Received data transfer message')
        self.submission_exporter.start_data_file_transfer(export.job_id, export.submission_uuid, export.project_uuid)
        self.logger.info('Started data transfer, informing ingest')
        self.ingest_service.set_data_file_transfer(export.job_id, DataTransferState.STARTED)
        self.logger.info('Acknowledging data transfer message')
        msg.ack()
