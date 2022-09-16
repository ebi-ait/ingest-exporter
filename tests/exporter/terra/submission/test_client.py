import unittest
import uuid
from unittest.mock import MagicMock

from google.cloud.pubsub_v1 import SubscriberClient

from exporter.terra.gcs.transfer import GcsTransfer
from exporter.terra.gcs.transfer_job import TransferJob
from exporter.terra.submission.client import TerraTransferClient


class TestTerraTransferClient(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_gcs_start = MagicMock()
        self.mock_gcs = MagicMock(spec=GcsTransfer)
        self.mock_gcs.start_job = self.mock_gcs_start

    def test_success(self):
        # Given
        upload_area = "s3fake://bucket/key"
        project_uuid = str(uuid.uuid4())
        export_job_id = str(uuid.uuid4())

        aws_id = 'aws_id'
        aws_secret = 'aws_secret'
        gcs_project = 'gcs_project'
        gcs_bucket = 'gcs_bucket'
        prefix = 'prefix'
        topic = 'topic'
        bucket, key = TerraTransferClient.bucket_and_key_for_upload_area(upload_area)

        test_job = TransferJob(
            name=f'transferJobs/{export_job_id}',
            description=f'Transfer job for ingest upload-service area {key} and export-job-id {export_job_id}',
            project_id=gcs_project,
            source_bucket=bucket,
            source_path=f'{key}/',
            aws_access_key_id=aws_id,
            aws_access_key_secret=aws_secret,
            dest_bucket=gcs_bucket,
            dest_path=f'{prefix}/{project_uuid}/data/',
            notification_topic=topic
        )
        terra = TerraTransferClient(
            self.mock_gcs,
            aws_id,
            aws_secret,
            gcs_project,
            gcs_bucket,
            prefix,
            topic
        )

        # When
        terra.transfer_data_files(upload_area, project_uuid, export_job_id)

        # Then
        self.mock_gcs_start.assert_called_once_with(test_job)

    def test_topic_is_expanded(self):
        # Given
        project = 'project'
        topic = 'topic'

        # When
        test_job = TransferJob('', '', project, '', '', '', '', '', '', topic)

        # Then
        self.assertNotEqual(topic, test_job.notification_topic)
        path = SubscriberClient.topic_path(project, topic)
        self.assertEqual(path, test_job.notification_topic)
