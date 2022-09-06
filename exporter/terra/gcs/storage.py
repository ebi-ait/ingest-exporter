from io import BufferedReader, StringIO
from time import sleep
from typing import Union, IO, Any

from google.api_core import retry
from google.api_core.exceptions import PreconditionFailed, ServiceUnavailable
from google.cloud.storage import Client, Blob, Bucket

from exporter.session_context import SessionContext
from exporter.terra.exceptions import UploadPollingException

Streamable = Union[BufferedReader, StringIO, IO[Any]]


class GcsStorage:
    def __init__(self, gcs_client: Client, bucket_name: str, storage_prefix: str):
        self.gcs_client = gcs_client
        self.bucket_name = bucket_name
        self.storage_prefix = storage_prefix

        self.logger = SessionContext.register_logger(__name__)

    def file_exists(self, object_key: str) -> bool:
        dest_key = f'{self.storage_prefix}/{object_key}'
        staging_bucket: Bucket = self.gcs_client.bucket(self.bucket_name)
        blob: Blob = staging_bucket.blob(dest_key)
        if not blob.exists():
            return False
        else:
            blob.reload()
            return blob.metadata is not None and blob.metadata.get("export_completed", False)

    def write(self, object_key: str, data_stream: Streamable):
        try:
            dest_key = f'{self.storage_prefix}/{object_key}'
            staging_bucket: Bucket = self.gcs_client.bucket(self.bucket_name)
            blob: Blob = staging_bucket.blob(dest_key, chunk_size=1024 * 256 * 20)

            if not blob.exists():
                blob.upload_from_file(data_stream, if_generation_match=0)
                self.mark_complete(blob)
            else:
                self.assert_file_uploaded(object_key)
        except PreconditionFailed as e:
            # With if_generation_match=0, this pre-condition failure indicates that another
            # export instance has began uploading this file. We should not attempt to upload
            # and instead poll for its completion
            self.assert_file_uploaded(object_key)

    def move_file(self, source_key: str, object_key: str):
        dest_key = f'{self.storage_prefix}/{object_key}'
        staging_bucket: Bucket = self.gcs_client.bucket(self.bucket_name)
        source_blob: Blob = staging_bucket.blob(source_key)

        new_blob = staging_bucket.rename_blob(source_blob, dest_key)
        self.mark_complete(new_blob)
        return

    def mark_complete(self, blob: Blob):
        blob.metadata = {"export_completed": True}
        patch_retryer = retry.Retry(predicate=retry.if_exception_type(ServiceUnavailable),
                                    deadline=60)

        patch_retryer(lambda: blob.patch())()

    def assert_file_uploaded(self, object_key: str):
        dest_key = f'{self.storage_prefix}/{object_key}'
        staging_bucket: Bucket = self.gcs_client.bucket(self.bucket_name)
        blob = staging_bucket.blob(dest_key)

        one_hour_in_seconds = 60 * 60
        one_hundred_milliseconds = 0.1
        return self._assert_file_uploaded(blob, one_hundred_milliseconds, one_hour_in_seconds)

    def _assert_file_uploaded(self, blob: Blob, sleep_time: float, max_sleep_time: float):
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
                return self._assert_file_uploaded(blob, new_sleep_time, max_sleep_time)
