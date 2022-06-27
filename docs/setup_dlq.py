import pika

# 1. Adds error queue and bindings
# 2. Deletes existing queues so that they can be recreated
# Prerequisite: port forward rabbit service: kubectl port-forward rabbit-0 5672:5672

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Create new error queue
channel.queue_declare(
    'ingest.exporter.errored.queue',
    durable=True,
    auto_delete=False
)
# create binding to new error queue on ingest.#.error
channel.queue_bind(
    exchange='ingest.exporter.exchange',
    queue='ingest.exporter.errored.queue',
    routing_key='ingest.#.error'
)

# Delete existing queue ingest.terra.experiments.new
channel.queue_delete('ingest.terra.experiments.new', if_empty=True)

# Delete existing queue ingest.manifests.assays.new
channel.queue_delete('ingest.manifests.assays.new', if_empty=True)

channel.close()
