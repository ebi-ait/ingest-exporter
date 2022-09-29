from unittest import TestCase

from kombu import Message
from mock import MagicMock

from exporter.ingest.export_job import DataTransferState
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
        self.mock_ingest_get = MagicMock()
        self.mock_ingest_get.return_value = self.mock_submission
        self.mock_ingest_set = MagicMock()
        mock_ingest = MagicMock(spec=IngestService)
        mock_ingest.get_submission = self.mock_ingest_get
        mock_ingest.set_data_file_transfer = self.mock_ingest_set

        self.producer = MagicMock()
        self.producer.publish = MagicMock()
        self.mock_gcs_start = MagicMock()
        mock_gcs = MagicMock(spec=GcsTransfer)
        mock_gcs.start_job = self.mock_gcs_start

        terra = TerraTransferClient(mock_gcs, 'aws_id', 'aws_secret', 'gcs_project', 'gcs_bucket', 'prefix', 'topic')
        exporter = TerraSubmissionExporter(mock_ingest, terra)

        handler = TerraSubmissionHandler(
            exporter,
            mock_ingest
        )
        self.listener = QueueListener(MagicMock(), handler, MagicMock())

        self.msg_ack = MagicMock()
        self.msg_reject = MagicMock()
        self.message = MagicMock(spec=Message)
        self.message.ack = self.msg_ack
        self.message.reject = self.msg_reject

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
        self.mock_ingest_get.assert_called_once_with("submissionUuid")
        self.mock_gcs_start.assert_called_once_with(test_job)
        self.mock_ingest_set.assert_called_once_with("exportJobId", DataTransferState.STARTED)
        self.msg_ack.assert_called_once()

    def test_missing_submit_action(self):
        # Given
        self.mock_submission["submitActions"] = []
        body = '{"exportJobId": "exportJobId", "submissionUuid": "submissionUuid", "projectUuid": "projectUuid", "callbackLink": "callbackLink", "context": {}}'

        # When
        self.listener.try_handle_or_reject(body, self.message)

        # Then
        self.mock_ingest_get.assert_called_once_with("submissionUuid")
        self.mock_gcs_start.assert_not_called()
        self.mock_ingest_set.assert_not_called()
        self.msg_reject.assert_called_once_with(requeue=False)

    def test_missing_staging_area(self):
        # Given
        self.mock_submission.pop("stagingDetails")
        body = '{"exportJobId": "exportJobId", "submissionUuid": "submissionUuid", "projectUuid": "projectUuid", "callbackLink": "callbackLink", "context": {}}'

        # When
        self.listener.try_handle_or_reject(body, self.message)

        # Then
        self.mock_ingest_get.assert_called_once_with("submissionUuid")
        self.mock_gcs_start.assert_not_called()
        self.mock_ingest_set.assert_not_called()
        self.msg_reject.assert_called_once_with(requeue=False)

    def test_unexpected_failure(self):
        # Given
        body = '{}'
        self.mock_gcs_start.side_effect = FileTransferCouldNotStart()

        # When
        self.listener.try_handle_or_reject(body, self.message)

        # Then
        self.mock_gcs_start.assert_called_once()
        self.mock_ingest_set.assert_not_called()
        self.msg_reject.assert_called_once_with(requeue=False)