import unittest
import uuid
from unittest.mock import MagicMock

from kombu import Message

from exporter.ingest.export_job import DataTransferState
from exporter.ingest.service import IngestService
from exporter.session_context import get_session_value
from exporter.terra.submission.exporter import TerraSubmissionExporter
from exporter.terra.submission.handler import TerraSubmissionHandler


class TestTerraSubmissionHandler(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_exporter_start = MagicMock()
        mock_exporter = MagicMock(spec=TerraSubmissionExporter)
        mock_exporter.start_data_file_transfer = self.mock_exporter_start

        self.mock_ingest_set = MagicMock()
        mock_ingest = MagicMock(spec=IngestService)
        mock_ingest.set_data_file_transfer = self.mock_ingest_set
        self.handler = TerraSubmissionHandler(mock_exporter, mock_ingest)

        self.msg_ack = MagicMock()
        self.msg_reject = MagicMock()
        self.message = MagicMock(spec=Message)
        self.message.ack = self.msg_ack
        self.message.reject = self.msg_reject

    def test_handle_message(self):
        # Given
        export_job = str(uuid.uuid4())
        submission = str(uuid.uuid4())
        project = str(uuid.uuid4())
        body = {
            "exportJobId": export_job,
            "submissionUuid": submission,
            "projectUuid": project
        }

        # When
        self.handler.handle_message(body, self.message)

        # Then
        self.mock_exporter_start.assert_called_once_with(export_job, submission, project)
        self.mock_ingest_set.assert_called_once_with(export_job, DataTransferState.STARTED)

    def test_get_context(self):
        export_job = str(uuid.uuid4())
        submission = str(uuid.uuid4())
        project = str(uuid.uuid4())
        body = {
            "exportJobId": export_job,
            "submissionUuid": submission,
            "projectUuid": project
        }

        # When
        with self.handler.set_context(body):
            # Then
            self.assertEqual(submission, get_session_value('submission_uuid'))
            self.assertEqual(export_job, get_session_value('export_job_id'))
            self.assertEqual(project, get_session_value('project_uuid'))
        # And Then
        self.assertEqual('n/a', get_session_value('submission_uuid'))
        self.assertEqual('n/a', get_session_value('export_job_id'))
        self.assertEqual('n/a', get_session_value('project_uuid'))
