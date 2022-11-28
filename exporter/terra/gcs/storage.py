import json
import logging
from io import BufferedReader, StringIO
from time import sleep
from typing import Union, IO, Any

from google.api_core.exceptions import PreconditionFailed, ServiceUnavailable
from google.api_core.retry import Retry, if_exception_type
from google.cloud.storage import Client, Blob, Bucket
from google.oauth2.service_account import Credentials

from exporter.terra.exceptions import UploadPollingException

Streamable = Union[BufferedReader, StringIO, IO[Any]]


class GcsStorage:
    def __init__(self, project_id: str, credentials_path: str, logger_name: str = __name__):
        with open(credentials_path) as source:
            info = json.load(source)
        credentials: Credentials = Credentials.from_service_account_info(info)
        self.client = Client(project=project_id, credentials=credentials)
        self.logger = logging.getLogger(logger_name)

    def write(self, bucket_name: str, key: str, data_stream: Streamable, overwrite=False):
        blob = self.__blob(bucket_name, key)
        if overwrite:
            self.__overwrite(blob, data_stream)
        else:
            self.__write(blob, data_stream)

    def delete(self, bucket_name: str, key: str):
        blob = self.__blob(bucket_name, key)
        if blob.exists():
            self.logger.info(f'File exists, Deleting: {blob.name}')
            blob.delete()
        else:
            self.logger.info(f'File does not exist, no need to delete: {blob.name}')

    def delete_all(self, bucket_name: str, prefix: str):
        for blob in self.__all_blobs(bucket_name, prefix):
            self.logger.info(f'Deleting: {blob.name}')
            blob.delete()

    def __blob(self, bucket_name: str, key: str) -> Blob:
        bucket: Bucket = self.client.bucket(bucket_name)
        return bucket.blob(key, chunk_size=1024 * 256 * 20)

    def __all_blobs(self, bucket_name: str, prefix: str):
        return self.client.list_blobs(bucket_name, prefix=prefix, delimiter='/')

    def __overwrite(self, blob: Blob, data_stream: Streamable):
        blob.upload_from_file(data_stream)
        self.__mark_complete(blob)

    def __write(self, blob: Blob, data_stream: Streamable):
        try:
            if not blob.exists():
                blob.upload_from_file(data_stream, if_generation_match=0)
                self.__mark_complete(blob)
            else:
                self.__assert_file_uploaded(blob)
        except PreconditionFailed as e:
            # With if_generation_match=0, this pre-condition failure indicates that another
            # export instance has began uploading this file. We should not attempt to upload
            # and instead poll for its completion
            self.__assert_file_uploaded(blob)

    @staticmethod
    def __mark_complete(blob: Blob):
        blob.metadata = {"export_completed": True}
        retry_patch = Retry(
            predicate=if_exception_type(ServiceUnavailable),
            deadline=600
        )
        retry_patch(lambda: blob.patch())()

    def __assert_file_uploaded(self, blob: Blob, sleep_time: float = 0.1, max_sleep_time: float = 60 * 60):
        if sleep_time > max_sleep_time:
            raise UploadPollingException(f'Could not verify completed upload for blob {blob.name} within maximum '
                                         f'wait time of {str(max_sleep_time)} seconds')
        else:
            sleep(sleep_time)
            blob.reload()

            export_completed = blob.metadata is not None and blob.metadata.get("export_completed")
            if export_completed:
                return
            else:
                new_sleep_time = sleep_time * 2
                self.logger.info(f'Verifying upload of blob {blob.name}. Waiting for {str(new_sleep_time)} seconds...')
                return self.__assert_file_uploaded(blob, new_sleep_time, max_sleep_time)
