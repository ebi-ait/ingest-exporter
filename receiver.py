from kombu.mixins import ConsumerProducerMixin
from kombu import Consumer
from typing import Type


class Worker(ConsumerProducerMixin):
    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues

    def get_consumers(self, consumer: Type[Consumer], channel):
        return [consumer(queues=self.queues,
                         callbacks=[self.on_message])]


class Receiver(Worker):
    pass
