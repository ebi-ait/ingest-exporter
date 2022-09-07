import json
from typing import Dict

import google_auth_httplib2
import googleapiclient.discovery
import httplib2
from google.oauth2.service_account import Credentials
from googleapiclient._auth import with_scopes
from googleapiclient.errors import HttpError

from .exceptions import FileTransferCouldNotStart, FileTransferAlreadyExists, \
    TransferOperationsParseError
from .transfer_job import TransferJob


class GcsTransfer:
    def __init__(self, credentials_path: str):
        with open(credentials_path) as source:
            credentials_file = json.load(source)
        self.credentials = Credentials.from_service_account_info(credentials_file)

    def start_job(self, transfer_job: TransferJob):
        with self.create_transfer_client(self.credentials) as client:
            try:
                client.transferJobs().create(body=transfer_job.to_dict()).execute()
            except HttpError as e:
                if e.resp.status == 409:
                    raise FileTransferAlreadyExists() from e
                else:
                    raise FileTransferCouldNotStart() from e

    def is_job_complete(self, project_id: str, job_name: str):
        with self.create_transfer_client(self.credentials) as client:
            response: Dict = client.transferOperations().list(
                name="transferOperations",
                filter=json.dumps({
                    "project_id": project_id,
                    "job_names": [job_name]
                })
            ).execute()
        try:
            operations = response.get("operations", [])
            operation = operations[0] if len(operations) > 0 else None
            return operation and operation.get('done', False)
        except (KeyError, IndexError) as e:
            raise TransferOperationsParseError(f'Failed to parse transferOperations') from e

    @staticmethod
    def create_transfer_client(credentials):
        # Since we are using threads, we must create a new HttpLib2 instance for every request
        # See: https://googleapis.github.io/google-api-python-client/docs/thread_safety.html

        # AuthorizedHttp requires credentials with scopes
        # When using googleapiclient.discovery.build without requestBuilder and using credentials directly,
        # the client adds the scopes for you automatically but not when using with requestBuilder and AuthorizedHttp
        credentials_with_scope: Credentials = with_scopes(credentials,
                                                          ['https://www.googleapis.com/auth/cloud-platform'])

        def build_request(http, *args, **kwargs):
            new_http = google_auth_httplib2.AuthorizedHttp(credentials_with_scope, http=httplib2.Http())
            return googleapiclient.http.HttpRequest(new_http, *args, **kwargs)

        authorized_http = google_auth_httplib2.AuthorizedHttp(credentials_with_scope, http=httplib2.Http())
        return googleapiclient.discovery.build('storagetransfer', 'v1', requestBuilder=build_request,
                                               http=authorized_http,
                                               cache_discovery=False)
