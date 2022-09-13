import json
from logging import Logger

from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from google.oauth2.service_account import Credentials

from exporter.ingest.service import IngestService
from exporter.session_context import SessionContext


class TerraTransferResponder:
    def __init__(self, ingest_service: IngestService, gcp_project: str, gcp_topic: str, gcp_credentials_path: str):
        self.ingest = ingest_service
        self.subscription_path = SubscriberClient.subscription_path(gcp_project, gcp_topic)
        self.topic_path = SubscriberClient.topic_path(gcp_project, gcp_topic)
        self.logger = SessionContext.register_logger(__name__)

        with open(gcp_credentials_path) as source:
            credentials_file = json.load(source)
        self.credentials = Credentials.from_service_account_info(credentials_file)
        with SubscriberClient(credentials=self.credentials) as subscriber:
            try:
                subscriber.create_subscription(name=self.subscription_path, topic=self.topic_path)
                self.logger.info(f'Subscription Created: {self.subscription_path}')
            except AlreadyExists:
                self.logger.info(f'Subscription Found: {self.subscription_path}')
            except Exception as e:
                self.logger.info(f'Cannot check whether subscription exists: {self.subscription_path} due to {e}')

    def listen(self):
        with SubscriberClient(credentials=self.credentials) as subscriber:
            future = subscriber.subscribe(self.subscription_path, callback=self.handle_message)
            try:
                self.logger.info(f'Running Google Data Transfer Listener')
                future.result()
            except Exception:
                future.cancel()

    def handle_message(self, message: Message):
        if message.attributes.get("eventType", "") != "TRANSFER_OPERATION_SUCCESS":
            message.nack()
        transfer_name = message.attributes.get("transferJobName", "")
        if not transfer_name.startswith('transferJobs/'):
            message.nack()
        export_job_id = transfer_name.replace('transferJobs/', '')
        with SessionContext(
            logger=self.logger,
            context={'export_job_id': export_job_id}
        ) as context:
            self.hande_data_transfer_complete(context.logger, message, export_job_id)

    def hande_data_transfer_complete(self, logger: Logger, message: Message, export_job_id: str):
        logger.info(f'Received message that data transfer is complete, informing ingest')
        self.ingest.set_data_file_transfer(export_job_id, 'COMPLETE')
        logger.info(f'Acknowledging data transfer complete message')
        message.ack()
