import os
from typing import Tuple

from exporter.terra.gcs.config import GcpConfig
from exporter.terra.gcs.transfer import GcsTransfer
from exporter.terra.gcs.transfer_job import TransferJob


class TerraTransferClient:
    def __init__(self, gcs_transfer: GcsTransfer, aws_access_key_id: str, aws_access_key_secret: str, gcs_project_id: str, gcs_dest_bucket: str, gcs_dest_prefix: str, notification_topic: str):
        self.gcs_transfer = gcs_transfer
        self.aws_access_key_id = aws_access_key_id
        self.aws_access_key_secret = aws_access_key_secret
        self.gcs_project_id = gcs_project_id
        self.gcs_dest_bucket = gcs_dest_bucket
        self.gcs_bucket_prefix = gcs_dest_prefix
        self.notification_topic = notification_topic

    @staticmethod
    def from_env():
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_access_key_secret = os.environ['AWS_ACCESS_KEY_SECRET']
        gcp_config = GcpConfig.from_env()
        gcs_transfer = GcsTransfer(gcp_config.gcp_credentials_path)
        terra_bucket_name = os.environ['TERRA_BUCKET_NAME']
        terra_bucket_prefix = os.environ['TERRA_BUCKET_PREFIX']

        return TerraTransferClient(gcs_transfer, aws_access_key_id, aws_access_key_secret, gcp_config.gcp_project, terra_bucket_name, terra_bucket_prefix, gcp_config.gcp_topic)

    def transfer_data_files(self, upload_area: str, project_uuid, export_job_id: str):
        transfer_job = self.__get_transfer_job(upload_area, project_uuid, export_job_id)
        self.gcs_transfer.start_job(transfer_job)

    def is_transfer_done(self, export_job_id: str):
        return self.gcs_transfer.is_job_complete(self.gcs_project_id, export_job_id)

    def __get_transfer_job(self, upload_area: str, project_uuid: str, export_job_id: str) -> TransferJob:
        source_bucket, upload_area_key = self.bucket_and_key_for_upload_area(upload_area)
        return TransferJob(
            name=f'transferJobs/{export_job_id}',
            description=f'Transfer job for ingest upload-service area {upload_area_key} and export-job-id {export_job_id}',
            project_id=self.gcs_project_id,
            source_bucket=source_bucket,
            source_path=f'{upload_area_key}/',
            aws_access_key_id=self.aws_access_key_id,
            aws_access_key_secret=self.aws_access_key_secret,
            dest_bucket=self.gcs_dest_bucket,
            dest_path=f'{self.gcs_bucket_prefix}/{project_uuid}/data/',
            notification_topic=self.notification_topic
        )

    @staticmethod
    def bucket_and_key_for_upload_area(upload_area: str) -> Tuple[str, str]:
        bucket_and_key_str = upload_area.split("//")[1]
        bucket_and_key_list = bucket_and_key_str.split("/", 1)
        return bucket_and_key_list[0], bucket_and_key_list[1].split("/")[0]


