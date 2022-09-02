from abc import ABC

from kombu import Message, Producer

from exporter.session_context import SessionContext


class MessageHandler(ABC):
    def __init__(self, logger_name: str):
        self.logger = SessionContext.register_logger(logger_name)
        self.producer = None

    def set_context(self, body: dict) -> SessionContext:
        return SessionContext(
            logger=self.logger,
            context={
                'submission_uuid': body.get('submissionUuid'),
                'export_job_id': body.get('exportJobId')
            }
        )

    def handle_message(self, body: dict, msg: Message):
        pass

    def add_producer(self, producer: Producer):
        self.logger.info(f'Running Listener')
        self.producer = producer
