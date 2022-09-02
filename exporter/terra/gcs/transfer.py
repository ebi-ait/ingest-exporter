import json
import logging
from typing import Dict

import google_auth_httplib2
import googleapiclient.discovery
import httplib2
from google.auth.credentials import Credentials
from googleapiclient._auth import with_scopes
from googleapiclient.errors import HttpError

from .exceptions import FileTransferCouldNotStart, FileTransferAlreadyExists
from .transfer_job import TransferJob


class GcsTransfer:
    def __init__(self, aws_access_key_id: str, aws_access_key_secret: str, project_id: str, gcs_dest_bucket: str, gcs_dest_prefix: str, credentials: Credentials):
        self.aws_access_key_id = aws_access_key_id
        self.aws_access_key_secret = aws_access_key_secret
        self.project_id = project_id
        self.gcs_dest_bucket = gcs_dest_bucket
        self.gcs_bucket_prefix = gcs_dest_prefix
        self.credentials = credentials

        self.client = self.create_transfer_client()
        self.logger = logging.getLogger(__name__)

    def transfer_upload_area(self, source_bucket: str, upload_area_key: str, project_uuid: str, export_job_id: str) -> TransferJob:
        transfer_job_spec = self.transfer_job_spec_for_upload_area(source_bucket, upload_area_key, project_uuid, export_job_id)
        try:
            self.client.transferJobs().create(body=transfer_job_spec.to_dict()).execute()
        except HttpError as e:
            if e.resp.status == 409:
                raise FileTransferAlreadyExists()
            else:
                raise FileTransferCouldNotStart()

        return transfer_job_spec

    def transfer_job_spec_for_upload_area(self, source_bucket: str, upload_area_key: str, project_uuid: str, export_job_id: str) -> TransferJob:
        return TransferJob(
            name=f'transferJobs/{export_job_id}',
            description=f'Transfer job for ingest upload-service area {upload_area_key} and export-job-id {export_job_id}',
            project_id=self.project_id,
            source_bucket=source_bucket,
            source_path=f'{upload_area_key}/',
            aws_access_key_id=self.aws_access_key_id,
            aws_access_key_secret=self.aws_access_key_secret,
            dest_bucket=self.gcs_dest_bucket,
            dest_path=f'{self.gcs_bucket_prefix}/{project_uuid}/data/'
        )

    def is_job_complete(self, job_name: str):
        request = self.client.transferOperations().list(
            name="transferOperations",
            filter=json.dumps({
                "project_id": self.project_id,
                "job_names": [job_name]
            })
        )
        response: Dict = request.execute()

        try:
            operations = response.get("operations",[])
            operation = operations[0] if len(operations) > 0 else None
            return operation and operation.get('done', False)

        except (KeyError, IndexError) as e:
            raise Exception(f'Failed to parse transferOperations') from e

    def create_transfer_client(self):
        # Since we are using threads, we must create a new HttpLib2 instance for every request
        # See: https://googleapis.github.io/google-api-python-client/docs/thread_safety.html

        # AuthorizedHttp requires credentials with scopes
        # When using googleapiclient.discovery.build without requestBuilder and using credentials directly,
        # the client adds the scopes for you automatically but not when using with requestBuilder and AuthorizedHttp
        credentials_with_scope: Credentials = with_scopes(self.credentials,
                                                          ['https://www.googleapis.com/auth/cloud-platform'])

        def build_request(http, *args, **kwargs):
            new_http = google_auth_httplib2.AuthorizedHttp(credentials_with_scope, http=httplib2.Http())
            return googleapiclient.http.HttpRequest(new_http, *args, **kwargs)

        authorized_http = google_auth_httplib2.AuthorizedHttp(credentials_with_scope, http=httplib2.Http())
        return googleapiclient.discovery.build('storagetransfer', 'v1', requestBuilder=build_request,
                                               http=authorized_http,
                                               cache_discovery=False)
