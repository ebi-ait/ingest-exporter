from kombu import Connection, Exchange, Queue, Consumer, Message, Producer

exchange = Exchange('ingest.exporter.exchange', type="topic")
error_queue = Queue(name='ingest.exporter.errored.queue', exchange=exchange, routing_key='ingest.terra.spreadsheet.error')
conn = Connection("amqp://localhost:5672/")
channel = conn.channel()


def process_errors(body: str, message: Message):
    routing_key = message.headers.get('x-death', [{}])[0].get('routing-keys', [''])[0]
    if routing_key:
        print(f"message re-published to {routing_key}: {body}")
        producer = Producer(exchange=exchange, channel=channel, routing_key=routing_key)
        producer.publish(body)
        message.ack()
    else:
        message.nack()


with Consumer(conn, queues=error_queue, callbacks=[process_errors], accept=["application/json;charset=UTF-8"]):
    conn.drain_events(timeout=2)
    channel.close()

