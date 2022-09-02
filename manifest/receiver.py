import json
import time
from typing import List, Type

from kombu import Consumer
from kombu.mixins import ConsumerProducerMixin

from exporter.queue.config import QueueConfig
from exporter.session_context import SessionContext
from manifest.exporter import ManifestExporter


class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queues, callback):
        self.connection = connection
        self.queues = queues
        self.callback = callback
        self.logger = SessionContext.register_logger('ManifestExporter')

    def get_consumers(self, consumer: Type[Consumer], channel):
        return [consumer(queues=self.queues, callbacks=[self.callback])]


class Receiver(Worker):
    def __init__(self, connection, queues, callback):
        super().__init__(connection, queues, callback)


class ManifestReceiver(Receiver):
    def __init__(self, connection, queues: List[QueueConfig], exporter: ManifestExporter, publish_config: QueueConfig):
        super().__init__(connection, [q.queue_from_config() for q in queues], self.on_message)
        self.publish_config = publish_config
        self.exporter = exporter

    def run(self, **kwargs):
        self.logger.info(f'Running {__name__}')
        super().run(**kwargs)

    def notify_state_tracker(self, body_dict):
        self.publish_config.send_message(self.producer, body_dict)
        self.logger.info("Notified!")

    def on_message(self, body, message):
        self.logger.info(f'Message received: {body}')
        success = False
        start = time.perf_counter()
        body_dict = json.loads(body)
        submission_uuid = body_dict["envelopeUuid"]
        with SessionContext(logger=self.logger, context={'submission_uuid': submission_uuid}):
            try:
                self.logger.info('process received ' + body_dict["callbackLink"])
                self.logger.info('process index: ' + str(
                    body_dict["index"]) + ', total processes: ' + str(
                    body_dict["total"]))

                self.exporter.export(process_uuid=body_dict["documentUuid"], submission_uuid=submission_uuid)
                success = True
            except Exception as e:
                self.logger.error(f"Rejecting export manifest message: {body} due to error: {str(e)}")
                message.reject(requeue=False)
                self.logger.exception(str(e))

            if success:
                self.logger.info(f"Notifying state tracker of completed manifest: {body}")
                self.notify_state_tracker(body_dict)
                self.logger.info(f'Acknowledging export manifest message: {body}')
                message.ack()
                end = time.perf_counter()
                time_to_export = end - start
                self.logger.info('Finished! ' + str(message.delivery_tag))
                self.logger.info('Export time (ms): ' + str(time_to_export))