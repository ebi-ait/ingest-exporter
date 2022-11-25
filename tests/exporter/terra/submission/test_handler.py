import pytest
import uuid
from unittest.mock import Mock

from assertpy import assert_that
from kombu import Message

from exporter.ingest.export_job import ExportContextState
from exporter.ingest.service import IngestService
from exporter.session_context import get_session_value
from exporter.terra.submission.exporter import TerraSubmissionExporter
from exporter.terra.submission.handler import TerraSubmissionHandler


class MockSubmissionHandler(TerraSubmissionHandler):
    def __init__(self, ingest_service: IngestService):
        # Skip TerraSubmissionHandler init, to mock TerraSubmissionExporter
        super(TerraSubmissionHandler, self).__init__(__name__)
        self.submission_exporter = Mock(spec=TerraSubmissionExporter)
        self.ingest_service = ingest_service


@pytest.fixture
def ingest():
    return Mock(spec=IngestService)


@pytest.fixture
def handler(ingest):
    ingest.job_exists_with_submission.return_value = True
    return MockSubmissionHandler(ingest)


@pytest.fixture
def missing_job_handler(ingest):
    ingest.job_exists_with_submission.return_value = False
    return MockSubmissionHandler(ingest)


@pytest.fixture
def message():
    return Mock(spec=Message)


@pytest.fixture
def export_job_id() -> str:
    return str(uuid.uuid4()).replace('-', '')


@pytest.fixture
def submission_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def project_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def body(export_job_id, submission_uuid, project_uuid) -> dict:
    return {
        'exportJobId': export_job_id,
        'submissionUuid': submission_uuid,
        'projectUuid': project_uuid
    }


def test_happy_path(ingest, handler, body, message, export_job_id, project_uuid, submission_uuid):
    # When
    handler.handle_message(body, message)

    # Then
    handler.submission_exporter.start_data_file_transfer.assert_called_once_with(export_job_id, submission_uuid, project_uuid)
    ingest.set_data_file_transfer.assert_called_once_with(export_job_id, ExportContextState.STARTED)
    message.ack.assert_called_once()


def test_missing_job_or_submission(ingest, missing_job_handler, body, message):
    # When
    missing_job_handler.handle_message(body, message)

    # Then
    message.ack.assert_called_once()
    missing_job_handler.submission_exporter.start_data_file_transfer.assert_not_called()
    ingest.set_data_file_transfer.assert_not_called()


def test_context(export_job_id, project_uuid, submission_uuid, handler, body):
    # When
    with handler.set_context(body):
        # Then
        assert_that(get_session_value('export_job_id')).is_equal_to(export_job_id)
        assert_that(get_session_value('project_uuid')).is_equal_to(project_uuid)
        assert_that(get_session_value('submission_uuid')).is_equal_to(submission_uuid)
    # And Then
    na = 'n/a'
    assert_that(get_session_value('export_job_id')).is_equal_to(na)
    assert_that(get_session_value('project_uuid')).is_equal_to(na)
    assert_that(get_session_value('submission_uuid')).is_equal_to(na)
