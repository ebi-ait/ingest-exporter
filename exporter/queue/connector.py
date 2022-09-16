from kombu import Connection

from exporter.queue.config import AmqpConnConfig
from exporter.queue.listener import QueueListener


class QueueConnector:
    def __init__(self, amqp_conn_config: AmqpConnConfig, listener: QueueListener):
        self.amqp_conn_config = amqp_conn_config
        self.listener = listener

    def run(self):
        with Connection(self.amqp_conn_config.broker_url()) as conn:
            self.listener.add_connection(conn)
            self.listener.run()
