from unittest import TestCase
from unittest.mock import MagicMock, Mock

from assertpy import assert_that
from ingest.api.ingestapi import IngestApi

from exporter.metadata import MetadataResource, MetadataParseException
from exporter.schema import SchemaService
from exporter.terra.dcp_staging_client import DcpStagingClient
from tests.mocks.files import MockEntityFiles
from tests.mocks.ingest import MockIngestAPI


class FileDescriptorTest(TestCase):
    def setUp(self) -> None:
        # Setup Entity Files Utility
        self.ingest = self.init_mock_ingest()
        self.schema_service = self.init_schema_service(self.ingest)
        self.mock_files = MockEntityFiles(base_uri='http://mock-ingest-api/')

    def test_when__null_dataFileUuid__fail(self):
        file_metadata = self.create_invalid_file()
        dcp_staging_client = self.init_dcp_staging_client()
        assert_that(dcp_staging_client.generate_file_desciptor_json) \
            .raises(MetadataParseException) \
            .when_called_with(file_metadata)

    def test_when__valid_file__pass(self):
        file_metadata = self.create_valid_file()
        dcp_staging_client = self.init_dcp_staging_client()
        dcp_staging_client.generate_file_desciptor_json(file_metadata)

    def create_invalid_file(self) -> MetadataResource:
        file = self.create_valid_file()
        file.full_resource["dataFileUuid"] = None
        return file

    def create_valid_file(self) -> MetadataResource:
        file_metadata = MetadataResource.from_dict(self.mock_files.get_entity('files', 'mock-analysis-output-file'))
        return file_metadata

    def init_dcp_staging_client(self):
        dcp_staging_client = DcpStagingClient(schema_service=self.schema_service,
                                              ingest_client=None,
                                              gcs_storage=None,
                                              gcs_xfer=None)
        return dcp_staging_client

    def init_schema_service(self, mock_ingest_api):
        test_ttl_seconds = 3
        test_schema_service = SchemaService(mock_ingest_api, ttl=test_ttl_seconds)
        return test_schema_service

    def init_mock_ingest(self):
        mock_ingest_api = MagicMock(spec=IngestApi)
        mock_ingest_api.get_schemas = Mock()
        mock_ingest_api.get_schemas.return_value = [
            {
                "_links": {"json-schema": {"href": "https://schema.humancellatlas.org/system/2.0.0/file_descriptor"}},
                "schemaVersion": "2.0.0"
            }
        ]
        return mock_ingest_api
