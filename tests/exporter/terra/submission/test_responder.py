import unittest
import uuid
from unittest.mock import Mock

from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message

from exporter.ingest.export_job import ExportContextState
from exporter.ingest.service import IngestService
from exporter.session_context import SessionContext
from exporter.terra.submission.responder import TerraTransferResponder


class MockTerraTransferResponder(TerraTransferResponder):
    # Not calling superclass to skip loading Credentials
    def __init__(self, ingest_service: IngestService, gcp_project: str, gcp_topic: str):
        self.ingest = ingest_service
        self.subscription_path = SubscriberClient.subscription_path(gcp_project, gcp_topic)
        self.topic_path = SubscriberClient.topic_path(gcp_project, gcp_topic)
        self.logger = SessionContext.register_logger(__name__)
        self.credentials = None


class TestTerraTransferResponder(unittest.TestCase):
    def setUp(self) -> None:
        self.project = 'project'
        self.topic = 'topic'
        self.ingest = Mock(spec=IngestService)
        self.message = Mock(spec=Message)
        self.responder = MockTerraTransferResponder(self.ingest, self.project, self.topic)

    def test_handle_message(self):
        # Given
        export_id = str(uuid.uuid4())
        self.message.attributes = {
            "eventType": "TRANSFER_OPERATION_SUCCESS",
            "transferJobName": "transferJobs/" + export_id
        }
        # when
        self.responder.handle_message(self.message)

        # Then
        self.ingest.job_exists_with_submission.assert_called_once_with(export_id)
        self.ingest.set_data_file_transfer.assert_called_once_with(export_id, ExportContextState.COMPLETE)
        self.message.ack.assert_called_once()
        self.message.nack.assert_not_called()

    def test_handle_empty_message_attributes(self):
        # Given
        self.message.attributes = {}

        # when
        self.responder.handle_message(self.message)

        # Then
        self.ingest.job_exists_with_submission.assert_not_called()
        self.ingest.set_data_file_transfer.assert_not_called()
        self.message.ack.assert_not_called()
        self.message.nack.assert_called_once()

    def test_handle_message_without_transfer_job(self):
        # Given
        self.message.attributes = {
            "eventType": "TRANSFER_OPERATION_SUCCESS"
        }
        # when
        self.responder.handle_message(self.message)

        # Then
        self.ingest.job_exists_with_submission.assert_not_called()
        self.ingest.set_data_file_transfer.assert_not_called()
        self.message.ack.assert_not_called()
        self.message.nack.assert_called_once()

    def test_handle_message_with_malformed_transfer_job(self):
        # Given
        export_id = str(uuid.uuid4())
        self.message.attributes = {
            "eventType": "TRANSFER_OPERATION_SUCCESS",
            "transferJobName": "transferBob/" + export_id
        }
        # when
        self.responder.handle_message(self.message)

        # Then
        self.ingest.job_exists_with_submission.assert_not_called()
        self.ingest.set_data_file_transfer.assert_not_called()
        self.message.ack.assert_not_called()
        self.message.nack.assert_called_once()

    def test_handle_message_for_other_server(self):
        # Given
        self.ingest.job_exists_with_submission.return_value = False

        export_id = str(uuid.uuid4())
        self.message.attributes = {
            "eventType": "TRANSFER_OPERATION_SUCCESS",
            "transferJobName": "transferJobs/" + export_id
        }

        # when
        self.responder.handle_message(self.message)

        # Then
        self.ingest.job_exists_with_submission.assert_called_once_with(export_id)
        self.ingest.set_data_file_transfer.assert_not_called()
        self.message.ack.assert_not_called()
        self.message.nack.assert_called_once()
