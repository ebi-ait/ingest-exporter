from dataclasses import dataclass, field

from kombu import Queue, Exchange, Producer


@dataclass
class AmqpConnConfig:
    host: str
    port: int

    def broker_url(self):
        return f'amqp://{self.host}:{str(self.port)}'


@dataclass
class QueueConfig:
    exchange: str
    routing_key: str
    name: str = field(default=None)
    exchange_type: str = field(default='topic')
    retry: bool = field(default=False)
    retry_policy: dict = field(default_factory=dict)
    queue_arguments: dict = field(default_factory=dict)

    def queue_from_config(self) -> Queue:
        exchange = Exchange(self.exchange, self.exchange_type)
        return Queue(self.name, exchange, self.routing_key, queue_arguments=self.queue_arguments)

    def send_message(self, producer: Producer, body: dict):
        producer.publish(
            body,
            exchange=self.exchange,
            routing_key=self.routing_key,
            retry=self.retry,
            retry_policy=self.retry_policy
        )
