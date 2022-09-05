import json
from typing import Dict, Tuple

from google.oauth2.service_account import Credentials

from exporter.terra.gcs.transfer import GcsTransfer
from exporter.terra.gcs.transfer_job import TransferJob


class TerraTransferClient:
    def __init__(self, aws_access_key_id: str, aws_access_key_secret: str, gcs_project_id: str, gcs_dest_bucket: str, gcs_dest_prefix: str, credentials_path: str):
        self.aws_access_key_id = aws_access_key_id
        self.aws_access_key_secret = aws_access_key_secret
        self.gcs_project_id = gcs_project_id
        self.gcs_dest_bucket = gcs_dest_bucket
        self.gcs_bucket_prefix = gcs_dest_prefix
        with open(credentials_path) as source:
            credentials_file = json.load(source)
        credentials = Credentials.from_service_account_info(credentials_file)
        self.gcs_transfer = GcsTransfer(credentials)

    def transfer_data_files(self, submission: Dict, project_uuid, export_job_id: str):
        upload_area = submission["stagingDetails"]["stagingAreaLocation"]["value"]
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
            dest_path=f'{self.gcs_bucket_prefix}/{project_uuid}/data/'
        )

    @staticmethod
    def bucket_and_key_for_upload_area(upload_area: str) -> Tuple[str, str]:
        bucket_and_key_str = upload_area.split("//")[1]
        bucket_and_key_list = bucket_and_key_str.split("/", 1)
        return bucket_and_key_list[0], bucket_and_key_list[1].split("/")[0]

