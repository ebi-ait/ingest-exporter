from unittest import TestCase

from exporter.metadata.checksums import FileChecksums
from exporter.metadata.datafile import DataFile


class DataFileTest(TestCase):
    @staticmethod
    def mock_checksums() -> FileChecksums:
        return FileChecksums("sha256", "crc32c", "sha1", "s3_etag")

    def test_parse_bucket_from_cloud_url(self):
        test_cloud_url = "s3://test-bucket/somefile.txt"
        test_data_file = DataFile("mock_uuid", "mock_version", "mock_file_name", test_cloud_url, "application/txt", "5",
                                  self.mock_checksums())
        self.assertEqual(test_data_file.source_bucket(), "test-bucket")

    def test_parse_key_from_cloud_url(self):
        test_cloud_url = "s3://test-bucket/somefile.txt"
        test_data_file = DataFile("mock_uuid", "mock_version", "mock_file_name", test_cloud_url, "application/txt", "5",
                                  self.mock_checksums())
        self.assertEqual(test_data_file.source_key(), "somefile.txt")

    def test_parse_nested_key_from_cloud_url(self):
        test_cloud_url = "s3://test-bucket/somedir/somesubdir/somefile.txt"
        test_data_file = DataFile("mock_uuid", "mock_version", "mock_file_name", test_cloud_url, "application/txt", "5",
                                  self.mock_checksums())
        self.assertEqual(test_data_file.source_key(), "somedir/somesubdir/somefile.txt")
