from unittest import TestCase

from kombu import Message
from mock import MagicMock

from exporter.ingest.service import IngestService
from exporter.queue.listener import QueueListener
from exporter.terra.gcs.exceptions import FileTransferCouldNotStart
from exporter.terra.gcs.transfer import GcsTransfer
from exporter.terra.gcs.transfer_job import TransferJob
from exporter.terra.submission.client import TerraTransferClient
from exporter.terra.submission.exporter import TerraSubmissionExporter
from exporter.terra.submission.handler import TerraSubmissionHandler


class TestTerraSubmissionExporterPackage(TestCase):
    def setUp(self) -> None:
        self.mock_submission = {
            "submitActions": ["Export"],
            "stagingDetails": {
                "stagingAreaLocation": {
                    "value": "s3fake://bucket/key"
                }
            }
        }
        self.mock_ingest = MagicMock(spec=IngestService)
        self.mock_ingest.get_submission = MagicMock()
        self.mock_ingest.get_submission.return_value = self.mock_submission
        self.mock_ingest.set_data_file_transfer = MagicMock()

        self.producer = MagicMock()
        self.producer.publish = MagicMock()
        self.mock_gcs = MagicMock(spec=GcsTransfer)
        self.mock_gcs.start_job = MagicMock()

        terra = TerraTransferClient(self.mock_gcs, 'aws_id', 'aws_secret', 'gcs_project', 'gcs_bucket', 'prefix', 'topic')
        exporter = TerraSubmissionExporter(self.mock_ingest, terra)

        handler = TerraSubmissionHandler(
            exporter,
            self.mock_ingest
        )
        self.listener = QueueListener(MagicMock(), handler, MagicMock())

        self.message = MagicMock(spec=Message)
        self.message.ack = MagicMock()
        self.message.reject = MagicMock()

    def test_package_success(self):
        # Given
        body = '{"exportJobId": "exportJobId", "submissionUuid": "submissionUuid", "projectUuid": "projectUuid", "callbackLink": "callbackLink", "context": {}}'
        test_job = TransferJob(
            name='transferJobs/exportJobId',
            description='Transfer job for ingest upload-service area key and export-job-id exportJobId',
            project_id='gcs_project',
            source_bucket='bucket',
            source_path='key/',
            aws_access_key_id='aws_id',
            aws_access_key_secret='aws_secret',
            dest_bucket='gcs_bucket',
            dest_path='prefix/projectUuid/data/',
            notification_topic='topic'
        )
        # When
        self.listener.try_handle_or_reject(body, self.message)

        # Then
        self.mock_ingest.get_submission.assert_called_once_with("submissionUuid")
        self.mock_gcs.start_job.assert_called_once_with(test_job)
        self.mock_ingest.set_data_file_transfer.assert_called_once_with("exportJobId", "STARTED")
        self.message.ack.assert_called_once()

    def test_expected_failure(self):
        # Given
        self.mock_submission["submitActions"] = []
        body = '{"exportJobId": "exportJobId", "submissionUuid": "submissionUuid", "projectUuid": "projectUuid", "callbackLink": "callbackLink", "context": {}}'

        # When
        self.listener.try_handle_or_reject(body, self.message)

        # Then
        self.mock_ingest.get_submission.assert_called_once_with("submissionUuid")
        self.mock_gcs.start_job.assert_not_called()
        self.mock_ingest.set_data_file_transfer.assert_not_called()
        self.message.reject.assert_called_once_with(requeue=False)

    def test_unexpected_failure(self):
        # Given
        body = '{}'
        self.mock_gcs.start_job.side_effect = FileTransferCouldNotStart()

        # When
        self.listener.try_handle_or_reject(body, self.message)

        # Then
        self.mock_gcs.start_job.assert_called_once()
        self.mock_ingest.set_data_file_transfer.assert_not_called()
        self.message.reject.assert_called_once_with(requeue=False)