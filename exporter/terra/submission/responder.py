import json

from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from google.oauth2.service_account import Credentials

from exporter.ingest.export_job import ExportContextState, ExportJob
from exporter.ingest.service import IngestService
from exporter.session_context import SessionContext
from exporter.terra.gcs.config import GcpConfig


class TerraTransferResponder:
    def __init__(self, ingest_service: IngestService, gcp_config: GcpConfig):
        self.ingest = ingest_service
        self.subscription_path = SubscriberClient.subscription_path(gcp_config.gcp_project, gcp_config.gcp_topic)
        topic_path = SubscriberClient.topic_path(gcp_config.gcp_project, gcp_config.gcp_topic)
        self.logger = SessionContext.register_logger("TerraTransferResponder")

        with open(gcp_config.gcp_credentials_path) as source:
            credentials_file = json.load(source)
        self.credentials = Credentials.from_service_account_info(credentials_file)
        with SubscriberClient(credentials=self.credentials) as subscriber:
            try:
                subscriber.create_subscription(name=self.subscription_path, topic=topic_path)
                self.logger.info(f'Subscription Created: {self.subscription_path}')
            except AlreadyExists:
                self.logger.info(f'Subscription Found: {self.subscription_path}')
            except Exception as e:
                self.logger.info(f'Cannot check whether subscription exists: {self.subscription_path} due to {str(e) if str(e) else e.__class__.__name__}')

    def listen(self):
        while True:
            with SubscriberClient(credentials=self.credentials) as subscriber:
                future = subscriber.subscribe(self.subscription_path, callback=self.handle_message)
                try:
                    self.logger.info(f'Running Google Data Transfer Listener')
                    future.result()
                except Exception as e:
                    self.logger.error(f'Google Data Transfer Listener stopped due to: {str(e) if str(e) else e.__class__.__name__}')
                    future.cancel()

    def handle_message(self, message: Message):
        if message.attributes.get("eventType", "") != "TRANSFER_OPERATION_SUCCESS":
            self.logger.error(f'Received unexpected message: {message.attributes}')
            return message.nack()
        transfer_name = message.attributes.get("transferJobName", "")
        if not transfer_name.startswith('transferJobs/'):
            self.logger.error(f'Could not parse message: {message.attributes}')
            return message.nack()
        export_job_id = transfer_name.replace('transferJobs/', '')
        with SessionContext(logger=self.logger, context={'export_job_id': export_job_id}):
            job = self.ingest.get_job_if_exists(export_job_id)
            if not job:
                self.logger.warning(f'Export Job does not exist on this environment, this could be because the terra staging environment is used for both dev and staging ingest')
                return message.nack()
            if not job.submission_id:
                self.logger.info(f'Export Job linked Submission deleted. Acknowledging message')
                return message.ack()
            if job.data_file_transfer == ExportContextState.COMPLETE:
                self.logger.info(f'Export Job data file transfer already complete. Acknowledging message')
                return message.ack()
            self.handle_data_transfer_complete(message, job)

    def handle_data_transfer_complete(self, message: Message, export_job: ExportJob):
        self.logger.info(f'Received message that data transfer is complete, informing ingest')
        self.ingest.set_data_file_transfer(export_job.job_id, ExportContextState.COMPLETE)
        self.logger.info(f'Acknowledging data transfer complete message')
        message.ack()
