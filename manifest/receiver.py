import json
import logging
import time
from typing import List, Type

from kombu import Consumer
from kombu.mixins import ConsumerProducerMixin

from exporter.queue.config import QueueConfig
from manifest.exporter import ManifestExporter


class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues

    def get_consumers(self, consumer: Type[Consumer], channel):
        return [consumer(queues=self.queues,
                         callbacks=[self.on_message])]


class Receiver(Worker):
    pass


class ManifestReceiver(Receiver):
    def __init__(self, connection, queues: List[QueueConfig], exporter: ManifestExporter, publish_config: QueueConfig):
        self.connection = connection
        self.queues = [q.queue_from_config() for q in queues]
        self.logger = logging.getLogger(f'{__name__}.ManifestReceiver')
        self.publish_config = publish_config
        self.exporter = exporter

    def run(self):
        self.logger.info("Running ManifestReceiver")
        super(ManifestReceiver, self).run()

    def notify_state_tracker(self, body_dict):
        self.publish_config.send_message(self.producer, body_dict)
        self.logger.info("Notified!")

    def on_message(self, body, message):
        self.logger.info(f'Message received: {body}')
        success = False
        start = time.perf_counter()
        body_dict = json.loads(body)

        try:
            self.logger.info('process received ' + body_dict["callbackLink"])
            self.logger.info('process index: ' + str(
                body_dict["index"]) + ', total processes: ' + str(
                body_dict["total"]))

            self.exporter.export(process_uuid=body_dict["documentUuid"], submission_uuid=body_dict["envelopeUuid"])
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