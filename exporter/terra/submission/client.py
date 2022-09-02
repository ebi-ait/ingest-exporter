import json
from typing import Dict, Tuple

from google.oauth2.service_account import Credentials

from exporter.terra.gcs.transfer import GcsTransfer
from exporter.terra.gcs.transfer_job import TransferJob


def transfer_client_from_gcs_info(
        service_account_credentials_path: str,
        gcp_project: str,
        bucket_name: str,
        bucket_prefix: str,
        aws_access_key_id: str,
        aws_access_key_secret: str
    ):
    with open(service_account_credentials_path) as source:
        info = json.load(source)
    credentials: Credentials = Credentials.from_service_account_info(info)
    return GcsTransfer(
        aws_access_key_id,
        aws_access_key_secret,
        gcp_project,
        bucket_name,
        bucket_prefix,
        credentials
    )


class TerraTransferClient:
    def __init__(self, gcs_xfer: GcsTransfer):
        self.gcs_xfer = gcs_xfer

    def transfer_data_files(self, submission: Dict, project_uuid, export_job_id: str) -> (TransferJob, bool):
        upload_area = submission["stagingDetails"]["stagingAreaLocation"]["value"]
        bucket_and_key = self.bucket_and_key_for_upload_area(upload_area)
        transfer_job_spec = self.gcs_xfer.transfer_upload_area(bucket_and_key[0], bucket_and_key[1], project_uuid, export_job_id)
        return transfer_job_spec

    @staticmethod
    def bucket_and_key_for_upload_area(upload_area: str) -> Tuple[str, str]:
        bucket_and_key_str = upload_area.split("//")[1]
        bucket_and_key_list = bucket_and_key_str.split("/", 1)
        return bucket_and_key_list[0], bucket_and_key_list[1].split("/")[0]

