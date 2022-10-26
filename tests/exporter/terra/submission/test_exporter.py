import unittest
import uuid
from unittest.mock import MagicMock

from requests import HTTPError

from exporter.ingest.service import IngestService
from exporter.terra.exceptions import SubmissionDoesNotHaveStagingArea, \
    SubmissionDoesNotHaveRequiredAction
from exporter.terra.transfer import TerraTransferClient
from exporter.terra.submission.exporter import TerraSubmissionExporter


class TestTerraSubmissionExporter(unittest.TestCase):
    def setUp(self) -> None:
        self.upload_area = "s3fake://bucket/key"
        self.mock_submission = {
            "submitActions": ["Export"],
            "stagingDetails": {
                "stagingAreaLocation": {
                    "value": self.upload_area
                }
            }
        }
        self.mock_ingest_get = MagicMock()
        self.mock_ingest_get.return_value = self.mock_submission
        self.mock_ingest = MagicMock(spec=IngestService)
        self.mock_ingest.get_submission = self.mock_ingest_get

        self.mock_terra_transfer = MagicMock()
        mock_terra = MagicMock(spec=TerraTransferClient)
        mock_terra.transfer_data_files = self.mock_terra_transfer

        self.exporter = TerraSubmissionExporter(self.mock_ingest, mock_terra)

    def test_exporter_success(self):
        # Given
        export_id = str(uuid.uuid4())
        submission_uuid = str(uuid.uuid4())
        project_uuid = str(uuid.uuid4())

        # When
        self.exporter.start_data_file_transfer(export_id, submission_uuid, project_uuid)

        # Then
        self.mock_ingest_get.assert_called_once_with(submission_uuid)
        self.mock_terra_transfer.assert_called_once_with(self.upload_area, project_uuid, export_id)

    def test_exporter_no_submission(self):
        # Given
        self.mock_ingest.get_submission.side_effect = HTTPError()
        export_id = str(uuid.uuid4())
        submission_uuid = str(uuid.uuid4())
        project_uuid = str(uuid.uuid4())

        with self.assertRaises(HTTPError):
            # When
            self.exporter.start_data_file_transfer(export_id, submission_uuid, project_uuid)

        # Then
        self.mock_ingest_get.assert_called_once_with(submission_uuid)
        self.mock_terra_transfer.assert_not_called()

    def test_exporter_missing_submit_action(self):
        # Given
        self.mock_submission["submitActions"] = []
        export_id = str(uuid.uuid4())
        submission_uuid = str(uuid.uuid4())
        project_uuid = str(uuid.uuid4())

        with self.assertRaises(SubmissionDoesNotHaveRequiredAction):
            # When
            self.exporter.start_data_file_transfer(export_id, submission_uuid, project_uuid)

        # Then
        self.mock_ingest_get.assert_called_once_with(submission_uuid)
        self.mock_terra_transfer.assert_not_called()

    def test_exporter_missing_staging_area(self):
        # Given
        self.mock_submission.pop("stagingDetails")
        export_id = str(uuid.uuid4())
        submission_uuid = str(uuid.uuid4())
        project_uuid = str(uuid.uuid4())

        with self.assertRaises(SubmissionDoesNotHaveStagingArea):
            # When
            self.exporter.start_data_file_transfer(export_id, submission_uuid, project_uuid)

        # Then
        self.mock_ingest_get.assert_called_once_with(submission_uuid)
        self.mock_terra_transfer.assert_not_called()
