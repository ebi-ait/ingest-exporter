import uuid
import pytest
from unittest.mock import Mock
from assertpy import assert_that

from kombu import Message

from exporter.ingest.export_job import ExportContextState
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
def ingest():
    return Mock(spec=IngestService)


@pytest.fixture
def handler(ingest):
    ingest.job_exists_with_submission.return_value = True
    return MockSpreadsheetHandler(ingest)


@pytest.fixture
def missing_job_handler(ingest):
    ingest.job_exists_with_submission.return_value = False
    return MockSpreadsheetHandler(ingest)


@pytest.fixture
def message():
    return Mock(spec=Message)


@pytest.fixture
def export_job_id() -> str:
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


def test_happy_path(ingest, handler, body, message, export_job_id, project_uuid, submission_uuid):
    # When
    handler.handle_message(body, message)

    # Then
    handler.exporter.export_spreadsheet.assert_called_once_with(project_uuid, submission_uuid)
    assert_that(ingest.set_spreadsheet_generation.call_count).is_equal_to(2)
    for call in ingest.set_spreadsheet_generation.call_args_list:
        assert_that(call.args[0]).is_equal_to(export_job_id)
    assert_that(ingest.set_spreadsheet_generation.call_args_list[0].args[1]).is_equal_to(ExportContextState.STARTED)
    assert_that(ingest.set_spreadsheet_generation.call_args_list[1].args[1]).is_equal_to(ExportContextState.COMPLETE)
    message.ack.assert_called_once()


def test_missing_job_or_submission(ingest, missing_job_handler, body, message):
    # When
    missing_job_handler.handle_message(body, message)

    # Then
    message.ack.assert_called_once()
    ingest.set_spreadsheet_generation.assert_not_called()
    missing_job_handler.exporter.export_spreadsheet.assert_not_called()
