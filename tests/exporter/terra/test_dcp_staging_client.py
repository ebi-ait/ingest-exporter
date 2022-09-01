from unittest import TestCase
from unittest.mock import MagicMock, Mock

from hca_ingest.api.ingestapi import IngestApi

from exporter.graph.graph_crawler import GraphCrawler
from exporter.metadata.exceptions import MetadataParseException
from exporter.metadata.resource import MetadataResource
from exporter.metadata.service import MetadataService
from exporter.schema import SchemaService
from exporter.terra.client import TerraClient
from tests.mocks.files import MockEntityFiles


class AbstractFileGenerationBaseTests:
    schema_url = 'not_set'

    def init_terra_client(self) -> TerraClient:
        terra_client = TerraClient(schema_service=self.schema_service,
                                         ingest_client=None,
                                         gcs_storage=None,
                                         gcs_xfer=None)
        return terra_client

    def setUp(self) -> None:
        # Setup Entity Files Utility
        self.ingest = self.init_mock_ingest()
        self.schema_service = self.init_schema_service(self.ingest)
        self.terra_client: TerraClient = self.init_terra_client()
        self.mock_files = MockEntityFiles(base_uri='http://mock-ingest-api/')

    def init_schema_service(self, mock_ingest_api):
        test_ttl_seconds = 3
        test_schema_service = SchemaService(mock_ingest_api, ttl=test_ttl_seconds)
        return test_schema_service

    def init_mock_ingest(self):
        mock_ingest_api = MagicMock(spec=IngestApi)
        mock_ingest_api.get_schemas = Mock()
        mock_ingest_api.get_schemas.return_value = [
            {
                "_links": {"json-schema": {"href": self.schema_url}},
                "schemaVersion": "2.0.0"
            }
        ]
        return mock_ingest_api

    def create_invalid_document(self):
        self.fail('should be implemented by sub classes')

    def create_valid_file(self):
        self.fail('should implemented by sub classes')

    def test_when__invalid_document__fail(self):
        file_generation_input = self.create_invalid_document()
        with self.assertRaises(MetadataParseException):
            self.file_generation_function(file_generation_input)

    def test_when__valid_document__pass(self):
        file_metadata = self.create_valid_file()
        self.file_generation_function(file_metadata)


class LinksJsonTest(AbstractFileGenerationBaseTests, TestCase):

    schema_url = "https://schema.humancellatlas.org/system/3.0.0/links"

    def setUp(self):
        super().setUp()
        self.file_generation_function = self.terra_client.generate_links_json

    def create_invalid_document(self):
        file = self.create_valid_file()
        list(file.links.values())[0].process_uuid = 'invalid_uuid'
        return file

    def create_valid_file(self):
        test_assay_process = MetadataResource.from_dict(self.mock_files.get_entity('processes', 'mock-assay-process'))
        test_project = MetadataResource.from_dict(self.mock_files.get_entity('projects', 'mock-project'))
        crawler = GraphCrawler(MetadataService(self.ingest))
        experiment_graph = crawler.generate_complete_experiment_graph(test_assay_process, test_project)
        return experiment_graph.links


class FileDescriptorTest(AbstractFileGenerationBaseTests, TestCase):

    schema_url = "https://schema.humancellatlas.org/system/2.0.0/file_descriptor"

    def setUp(self):
        super().setUp()
        self.file_generation_function = self.terra_client.generate_file_descriptor_json

    def create_invalid_document(self):
        file = self.create_valid_file()
        file.full_resource["dataFileUuid"] = None
        return file

    def create_valid_file(self):
        file_metadata = MetadataResource.from_dict(self.mock_files.get_entity('files', 'mock-analysis-output-file'))
        return file_metadata
