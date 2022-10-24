from kombu import Message

from exporter.ingest.service import IngestService
from exporter.ingest.spreadsheet.message import SpreadsheetGeneratorMessage
from exporter.queue.handler import MessageHandler
from exporter.session_context import SessionContext


class SpreadsheetHandler(MessageHandler):
    def __init__(self, ingest_service: IngestService):
        super().__init__('IngestSpreadsheetGenerator')
        self.ingest = ingest_service

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
        message = SpreadsheetGeneratorMessage(body)
        self.logger.info('Received spreadsheet generation message')
        gen_job_id = self.ingest.start_spreadsheet_generation(message.submission_uuid)
        self.logger.info('Started spreadsheet generation, informing ingest')
        self.ingest.set_spreadsheet_generation_id(message.job_id, gen_job_id)
        self.logger.info('Acknowledging data transfer message')
        msg.ack()
