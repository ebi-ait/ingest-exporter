import json

from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from google.oauth2.service_account import Credentials

from exporter.ingest.service import IngestService
from exporter.session_context import SessionContext


class TerraTransferResponder:
    def __init__(self, ingest_service: IngestService, gcs_project_id: str, gcs_topic: str, gcs_credentials_path: str):
        self.ingest = ingest_service
        self.project_id = gcs_project_id
        self.topic = gcs_topic
        self.logger = SessionContext.register_logger(__name__)

        with open(gcs_credentials_path) as source:
            credentials_file = json.load(source)
        credentials = Credentials.from_service_account_info(credentials_file)
        subscriber = SubscriberClient(credentials=credentials)
        subscription_path = subscriber.subscription_path(self.project_id, self.topic)
        self.future = subscriber.subscribe(subscription_path, callback=self.handle_message)

    def listen(self):
        self.future.result()

    def handle_message(self, message: Message):
        if message.attributes.get("eventType", "") != "TRANSFER_OPERATION_SUCCESS":
            message.nack()
        transfer_name = message.attributes.get("transferJobName", "")
        if not transfer_name.startswith('transferJobs/'):
            message.nack()
        export_job_id = transfer_name.removeprefix('transferJobs/')
        self.logger.info(f'Received message from google that data transfer for export job {export_job_id} is finished.')
        self.ingest.set_data_file_transfer(export_job_id, 'COMPLETE')
        message.ack()
