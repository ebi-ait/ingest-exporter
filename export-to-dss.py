#!/usr/bin/env python
"""
This script listens on a ingest submission queue and as submission are completed will
call the ingest export service to generate the bundles and submit bundles to datastore
"""
__author__ = "jupp"
__license__ = "Apache 2.0"


from optparse import OptionParser
import os, sys, json
import time
import logging
from kombu import Connection, Exchange, Queue
from receiver import IngestReceiver


DEFAULT_RABBIT_URL=os.path.expandvars(os.environ.get('RABBIT_URL', 'amqp://localhost:5672'))
EXCHANGE = 'ingest.assays.exchange'
EXCHANGE_TYPE = 'topic'
QUEUE = 'ingest.assays.bundle.create'
ROUTING_KEY = 'ingest.assays.submitted'



def initReceivers(options):
    logger = logging.getLogger(__name__)

    receiver = IngestReceiver()

    def callback(body, message):
        success = False

        try:
            receiver.run(json.loads(body))
            success = True
        except Exception, e:
            message.reject(requeue=True)
            logger.exception(str(e))
            logger.info('Nacked! ' + str(message.delivery_tag))

        if success:
            message.ack()
            logger.info('Acked! ' + str(message.delivery_tag))



    assayExchange = Exchange(EXCHANGE, EXCHANGE_TYPE, passive=True, durable=False)
    assayCreatedQueue = Queue(QUEUE, exchange=assayExchange, routing_key=ROUTING_KEY, durable=False)


    with Connection(DEFAULT_RABBIT_URL, connect_timeout=1000, heartbeat=1000) as conn:
        # consume
        with conn.Consumer(assayCreatedQueue, callbacks=[callback]) as consumer:
            # Process messages and handle events on all channels
            while True:
                conn.drain_events()
                time.sleep(990)
                conn.heartbeat_check()


if __name__ == '__main__':
    format = ' %(asctime)s  - %(name)s - %(levelname)s in %(filename)s:%(lineno)s %(funcName)s(): %(message)s'
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=format)

    parser = OptionParser()
    parser.add_option("-q", "--queue", help="name of the ingest queues to listen for submission")
    parser.add_option("-r", "--rabbit", help="the URL to the Rabbit MQ messaging server")
    parser.add_option("-l", "--log", help="the logging level", default='INFO')

    (options, args) = parser.parse_args()
    initReceivers(options)

