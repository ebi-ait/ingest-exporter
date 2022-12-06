import uuid
import pytest
from unittest.mock import Mock

from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message

from exporter.ingest.export_job import ExportContextState, ExportJob
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


@pytest.fixture
def gcp_project():
    return 'project'


@pytest.fixture
def gcp_topic():
    return 'topic'


@pytest.fixture
def export_job_id() -> str:
    return str(uuid.uuid4()).replace('-', '')


@pytest.fixture
def submission_id() -> str:
    return str(uuid.uuid4()).replace('-', '')


@pytest.fixture
def job(mock_ingest, export_job_id, submission_id) -> ExportJob:
    job = ExportJob({})
    job.job_id = export_job_id
    job.submission_id = submission_id
    job.data_file_transfer = ExportContextState.STARTED
    mock_ingest.get_job_if_exists.return_value = job
    return job


@pytest.fixture
def job_does_not_exist(mock_ingest):
    mock_ingest.get_job_if_exists.return_value = None


@pytest.fixture
def job_without_submission(mock_ingest, export_job_id) -> ExportJob:
    job = ExportJob({})
    job.job_id = export_job_id
    mock_ingest.get_job_if_exists.return_value = job
    return job


@pytest.fixture
def complete_job(mock_ingest, export_job_id, submission_id) -> ExportJob:
    job = ExportJob({})
    job.job_id = export_job_id
    job.submission_id = submission_id
    job.data_file_transfer = ExportContextState.COMPLETE
    mock_ingest.get_job_if_exists.return_value = job
    return job

@pytest.fixture
def mock_ingest():
    return Mock(spec=IngestService)


@pytest.fixture
def responder(mock_ingest, gcp_project, gcp_topic):
    return MockTerraTransferResponder(mock_ingest, gcp_project, gcp_topic)


@pytest.fixture
def message(export_job_id):
    msg = Mock(spec=Message)
    msg.attributes = {
        "eventType": "TRANSFER_OPERATION_SUCCESS",
        "transferJobName": f'transferJobs/{export_job_id}'
    }
    return msg


@pytest.fixture(params=[
    {},
    {"eventType": "TRANSFER_OPERATION_SUCCESS"},
    {"eventType": "TRANSFER_OPERATION_SUCCESS", "transferJobName": "transferBob"}
], ids=['empty message', 'missing job id', 'malformed job id'])
def malformed_message(request):
    msg = Mock(spec=Message)
    msg.attributes = request.param
    return msg


def test_expected_message(responder, message, mock_ingest, job: ExportJob):
    # When
    responder.handle_message(message)

    # Then
    mock_ingest.get_job_if_exists.assert_called_once_with(job.job_id)
    mock_ingest.set_data_file_transfer.assert_called_once_with(job.job_id, ExportContextState.COMPLETE)
    message.ack.assert_called_once()

    message.nack.assert_not_called()


def test_malformed_message(responder, malformed_message, mock_ingest, job: ExportJob):
    # When
    responder.handle_message(malformed_message)

    # Then
    malformed_message.nack.assert_called_once()

    mock_ingest.get_job_if_exists.assert_not_called()
    mock_ingest.set_data_file_transfer.assert_not_called()
    malformed_message.ack.assert_not_called()


def test_message_from_other_server(responder, message, mock_ingest, job_does_not_exist: ExportJob):
    # When
    responder.handle_message(message)

    # Then
    message.nack.assert_called_once()

    mock_ingest.set_data_file_transfer.assert_not_called()
    message.ack.assert_not_called()


def test_message_for_deleted_submission(responder, message, mock_ingest, job_without_submission: ExportJob):
    # When
    responder.handle_message(message)

    # Then
    message.ack.assert_called_once()

    mock_ingest.set_data_file_transfer.assert_not_called()
    message.nack.assert_not_called()


def test_message_for_completed_job(responder, message, mock_ingest, complete_job: ExportJob):
    # When
    responder.handle_message(message)

    # Then
    message.ack.assert_called_once()

    mock_ingest.set_data_file_transfer.assert_not_called()
    message.nack.assert_not_called()
