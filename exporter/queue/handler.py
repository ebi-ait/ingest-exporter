from abc import ABC

from kombu import Message, Producer


class MessageHandler(ABC):
    def __init__(self):
        self.producer = None

    def handle_message(self, body: str, msg: Message):
        pass

    def add_producer(self, producer: Producer):
        self.producer = producer
