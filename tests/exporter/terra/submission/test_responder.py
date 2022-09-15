import unittest
import uuid
from unittest.mock import MagicMock

from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message

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
        self.mock_ingest_set = MagicMock()
        mock_ingest = MagicMock(spec=IngestService)
        mock_ingest.set_data_file_transfer = self.mock_ingest_set

        self.responder = MockTerraTransferResponder(mock_ingest, self.project, self.topic)

        self.msg_ack = MagicMock()
        self.msg_nack = MagicMock()
        self.message = MagicMock(spec=Message)
        self.message.ack = self.msg_ack
        self.message.nack = self.msg_nack

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
        self.mock_ingest_set.assert_called_once_with(export_id, 'COMPLETE')
        self.msg_ack.assert_called_once()
        self.msg_nack.assert_not_called()

    def test_handle_empty_message_attributes(self):
        # Given
        self.message.attributes = {}

        # when
        self.responder.handle_message(self.message)

        # Then
        self.mock_ingest_set.assert_not_called()
        self.msg_ack.assert_not_called()
        self.msg_nack.assert_called_once()

    def test_handle_message_without_transfer_job(self):
        # Given
        self.message.attributes = {
            "eventType": "TRANSFER_OPERATION_SUCCESS"
        }
        # when
        self.responder.handle_message(self.message)

        # Then
        self.mock_ingest_set.assert_not_called()
        self.msg_ack.assert_not_called()
        self.msg_nack.assert_called_once()

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
        self.mock_ingest_set.assert_not_called()
        self.msg_ack.assert_not_called()
        self.msg_nack.assert_called_once()
