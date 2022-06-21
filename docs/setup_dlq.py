import pika

# The x-dead-letter-exchange and x-dead-letter-routing-key arguments can only be added to a queue upon creation.
# Therefore, to an existing queue the que must be deleted and recreated
# When the queue is recreated any bindings will have been lost, so these must be recreated as well.

# Prerequisites
# first: scale down the exporter: kubectl scale deployment/ingest-exporter --replicas=0
# second: port forward rabbit service: kubectl port-forward rabbit-0 5672:5672

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
# recreate ingest.terra.experiments.new with existing bindings
channel.queue_declare(
    'ingest.terra.experiments.new',
    durable=True,
    auto_delete=False,
    arguments={
        'x-dead-letter-exchange': 'ingest.exporter.exchange',
        'x-dead-letter-routing-key': 'ingest.terra.experiment.error'
    }
)
channel.queue_bind(
    exchange='ingest.exporter.exchange',
    routing_key='ingest.exporter.experiment.submitted',
    queue='ingest.terra.experiments.new',
)

# Delete existing queue ingest.manifests.assays.new
channel.queue_delete('ingest.manifests.assays.new', if_empty=True)
# recreate ingest.manifests.assays.new with existing bindings
channel.queue_declare(
    'ingest.manifests.assays.new',
    durable=True,
    auto_delete=False,
    arguments={
        'x-dead-letter-exchange': 'ingest.exporter.exchange',
        'x-dead-letter-routing-key': 'ingest.manifest.assay.error'
    }
)
channel.queue_bind(
    exchange='ingest.exporter.exchange',
    routing_key='ingest.exporter.manifest.submitted',
    queue='ingest.manifests.assays.new',
)

channel.close()

# now scale up the exporter: kubectl scale deployment/ingest-exporter --replicas=X
