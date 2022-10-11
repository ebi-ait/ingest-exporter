import json
from concurrent.futures import ThreadPoolExecutor
from typing import Type, List

from kombu import Connection, Consumer, Message
from kombu.mixins import ConsumerProducerMixin

from exporter.queue.config import QueueConfig
from exporter.queue.handler import MessageHandler


class QueueListener(ConsumerProducerMixin):
    def __init__(self, watch_queue: QueueConfig, handler: MessageHandler, executor: ThreadPoolExecutor = None):
        self.connection = None
        self.watch_queue = watch_queue
        self.handler = handler
        self.executor = executor if executor else ThreadPoolExecutor()

    def add_connection(self, connection: Connection):
        self.connection = connection
        self.handler.add_producer(self.producer)

    def get_consumers(self, _consumer: Type[Consumer], channel) -> List[Consumer]:
        experiment_consumer = _consumer(
            [self.watch_queue.queue_from_config()],
            callbacks=[self.experiment_message_handler],
            prefetch_count=1
        )
        return [experiment_consumer]

    def experiment_message_handler(self, body: str, msg: Message):
        return self.executor.submit(lambda: self.try_handle_or_reject(body, msg))

    def try_handle_or_reject(self, body: str, msg: Message):
        json_body: dict = json.loads(body)
        with self.handler.set_context(json_body) as s:
            s.logger.info(f'Message received')
            try:
                self.handler.handle_message(json_body, msg)
            except Exception as e:
                s.logger.error(f"Rejecting message: {body} due to error: {str(e) if str(e) else e.__class__.__name__}")
                msg.reject(requeue=False)
                s.logger.exception(e)
