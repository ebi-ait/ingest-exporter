from kombu import Message

from exporter.ingest.export_job import ExportContextState
from exporter.ingest.service import IngestService
from exporter.queue.handler import MessageHandler
from exporter.session_context import SessionContext
from exporter.terra.storage import TerraStorageClient

from .exporter import SpreadsheetExporter
from .message import SpreadsheetExporterMessage


class SpreadsheetHandler(MessageHandler):
    def __init__(self, ingest_service: IngestService, terra_client:  TerraStorageClient, logger_name: str = __name__):
        super().__init__(logger_name)
        self.ingest = ingest_service
        self.exporter = SpreadsheetExporter(ingest_service, terra_client, logger_name)

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
        message = SpreadsheetExporterMessage(body)
        job = self.ingest.get_job(message.job_id)
        if not job.submission_id:
            self.logger.info(f'Received spreadsheet export message for deleted Submission. Acknowledging message')
            return msg.ack()
        if job.spreadsheet_generation == ExportContextState.COMPLETE:
            self.logger.info(f'Export Job spreadsheet generation already complete. Acknowledging message')
            return msg.ack()
        self.logger.info('Received spreadsheet export message, informing ingest')
        self.ingest.set_spreadsheet_generation(message.job_id, ExportContextState.STARTED)
        self.exporter.export_spreadsheet(message.project_uuid, message.submission_uuid)
        self.logger.info('Spreadsheet export finished, informing ingest')
        self.ingest.set_spreadsheet_generation(message.job_id, ExportContextState.COMPLETE)
        self.logger.info('Acknowledging spreadsheet export message')
        msg.ack()
