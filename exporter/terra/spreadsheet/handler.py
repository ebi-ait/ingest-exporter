from kombu import Message

from exporter.ingest.service import IngestService
from exporter.queue.handler import MessageHandler
from exporter.session_context import SessionContext
from exporter.terra.experiment.client import TerraStorageClient
from .exporter import SpreadsheetExporter
from .message import SpreadsheetExporterMessage


class SpreadsheetHandler(MessageHandler):
    def __init__(self, ingest_service: IngestService, terra_client:  TerraStorageClient):
        super().__init__('IngestSpreadsheetGenerator')
        self.exporter = SpreadsheetExporter(ingest_service, terra_client)

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
        self.logger.info('Received spreadsheet export message')
        self.exporter.export_spreadsheet(message.job_id, message.project_uuid, message.submission_uuid)
        self.logger.info('Acknowledging spreadsheet export message')
        msg.ack()
