import uuid
import pytest
from unittest.mock import Mock
from assertpy import assert_that

from kombu import Message

from exporter.ingest.export_job import ExportContextState, ExportJob
from exporter.ingest.service import IngestService
from exporter.terra.spreadsheet.exporter import SpreadsheetExporter
from exporter.terra.spreadsheet.handler import SpreadsheetHandler


class MockSpreadsheetHandler(SpreadsheetHandler):
    def __init__(self, ingest_service: IngestService):
        # Skip SpreadsheetHandler init, to mock SpreadsheetExporter
        super(SpreadsheetHandler, self).__init__(__name__)
        self.ingest = ingest_service
        self.exporter = Mock(spec=SpreadsheetExporter)


@pytest.fixture
def mock_ingest():
    return Mock(spec=IngestService)


@pytest.fixture
def job(mock_ingest, export_job_id, submission_id) -> ExportJob:
    job = ExportJob({})
    job.job_id = export_job_id
    job.submission_id = submission_id
    mock_ingest.get_job.return_value = job
    return job


@pytest.fixture
def job_without_submission(mock_ingest, export_job_id) -> ExportJob:
    job = ExportJob({})
    job.job_id = export_job_id
    mock_ingest.get_job.return_value = job
    return job


@pytest.fixture
def complete_job(mock_ingest, export_job_id, submission_id) -> ExportJob:
    job = ExportJob({})
    job.job_id = export_job_id
    job.submission_id = submission_id
    job.spreadsheet_generation = ExportContextState.COMPLETE
    mock_ingest.get_job.return_value = job
    return job


@pytest.fixture
def handler(mock_ingest):
    return MockSpreadsheetHandler(mock_ingest)


@pytest.fixture
def message():
    return Mock(spec=Message)


@pytest.fixture
def export_job_id() -> str:
    return str(uuid.uuid4()).replace('-', '')


@pytest.fixture
def submission_id() -> str:
    return str(uuid.uuid4()).replace('-', '')


@pytest.fixture
def submission_uuid():
    return str(uuid.uuid4())


@pytest.fixture
def project_uuid():
    return str(uuid.uuid4())


@pytest.fixture
def body(export_job_id, submission_uuid, project_uuid) -> dict:
    return {
        'exportJobId': export_job_id,
        'submissionUuid': submission_uuid,
        'projectUuid': project_uuid
    }


def test_happy_path(mock_ingest, handler, body, message, job, project_uuid, submission_uuid):
    # When
    handler.handle_message(body, message)

    # Then
    handler.exporter.export_spreadsheet.assert_called_once_with(project_uuid, submission_uuid)
    assert_that(mock_ingest.set_spreadsheet_generation.call_count).is_equal_to(2)
    for call in mock_ingest.set_spreadsheet_generation.call_args_list:
        assert_that(call.args[0]).is_equal_to(job.job_id)
    assert_that(mock_ingest.set_spreadsheet_generation.call_args_list[0].args[1]).is_equal_to(ExportContextState.STARTED)
    assert_that(mock_ingest.set_spreadsheet_generation.call_args_list[1].args[1]).is_equal_to(ExportContextState.COMPLETE)
    message.ack.assert_called_once()


def test_missing_job_or_submission(mock_ingest, handler, body, message, job_without_submission):
    # When
    handler.handle_message(body, message)

    # Then
    message.ack.assert_called_once()
    mock_ingest.set_spreadsheet_generation.assert_not_called()
    handler.exporter.export_spreadsheet.assert_not_called()


def test_complete_job(mock_ingest, handler, body, message, complete_job):
    # When
    handler.handle_message(body, message)

    # Then
    message.ack.assert_called_once()
    mock_ingest.set_spreadsheet_generation.assert_not_called()
    handler.exporter.export_spreadsheet.assert_not_called()
