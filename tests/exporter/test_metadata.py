import uuid
from dataclasses import asdict
from unittest import TestCase
from unittest.mock import patch, MagicMock, Mock

from exporter import utils
from exporter.metadata.checksums import FileChecksums
from exporter.metadata.datafile import DataFile
from exporter.metadata.exceptions import MetadataParseException
from exporter.metadata.provenance import MetadataProvenance
from exporter.metadata.resource import MetadataResource
from exporter.scratch import MetadataService


class MetadataResourceTest(TestCase):
    def test_provenance_from_dict(self):
        # given:
        uuid_value = str(uuid.uuid4())
        data = self._create_test_data(uuid_value)
        data['content']['describedBy'] = 'https://some-schema/1.2.3'

        # when:
        metadata_provenance = MetadataProvenance.from_dict(data)

        # then:
        self.assertIsNotNone(metadata_provenance)
        self.assertEqual(data['uuid']['uuid'], metadata_provenance.document_id)
        self.assertEqual('a date', metadata_provenance.submission_date)
        self.assertEqual('another date', metadata_provenance.update_date)
        self.assertEqual(1, metadata_provenance.schema_major_version)
        self.assertEqual(2, metadata_provenance.schema_minor_version)

    def test_provenance_from_dict_older_version(self):
        # given:
        uuid_value = str(uuid.uuid4())
        data = self._create_test_data(uuid_value)
        data['content']['describedBy'] = 'https://13.1.1/cell_suspension'

        # when:
        metadata_provenance = MetadataProvenance.from_dict(data)

        # then:
        self.assertIsNotNone(metadata_provenance)
        self.assertIsNone(asdict(metadata_provenance).get('schema_major_version'))
        self.assertIsNone(asdict(metadata_provenance).get('schema_minor_version'))

    def test_provenance_from_dict_newer_version(self):
        # given:
        uuid_value = str(uuid.uuid4())
        data = self._create_test_data(uuid_value)
        data['content']['describedBy'] = 'https://15.1.1/cell_suspension'

        # when:
        metadata_provenance = MetadataProvenance.from_dict(data)

        # then:
        self.assertEqual(15, metadata_provenance.schema_major_version)
        self.assertEqual(1, metadata_provenance.schema_minor_version)

    def test_provenance_from_dict_fail_fast(self):
        # given:
        uuid_value = str(uuid.uuid4())
        data = {'uuid': uuid_value,  # unexpected structure structure
                'submissionDate': 'a submission date',
                'updateDate': 'an update date'}

        # then:
        with self.assertRaises(MetadataParseException):
            # when
            MetadataProvenance.from_dict(data)

    def test_from_dict(self):
        # given:
        uuid_value = str(uuid.uuid4())
        data = self._create_test_data(uuid_value)

        # when:
        metadata = MetadataResource.from_dict(data)

        # then:
        self.assertIsNotNone(metadata)
        self.assertEqual('biomaterial', metadata.metadata_type)
        self.assertEqual(data['content'], metadata.metadata_json)
        self.assertEqual(utils.to_dcp_version(data['dcpVersion']), metadata.dcp_version)

        # and:
        self.assertEqual(uuid_value, metadata.uuid)

    def test_from_dict_fail_fast_with_missing_info(self):
        # given:
        data = {}

        # then:
        with self.assertRaises(MetadataParseException):
            # when
            MetadataResource.from_dict(data)

    def test_string_output_for_logging(self):
        # given
        data = self._create_test_data(str(uuid.uuid4()))
        meta = MetadataResource.from_dict(data)

        # when
        meta_str = meta.__repr__()

        # then
        self.assertFalse(meta_str.startswith('<exporter.metadata.MetadataResource'))

    def test_full_resource_not_included_on_repr(self):
        # given
        data = self._create_test_data(str(uuid.uuid4()))
        meta = MetadataResource.from_dict(data)
        self.assertDictEqual(meta.metadata_json, meta.full_resource.get('content'))
        # when
        meta_str = meta.__repr__()
        # then
        self.assertRaises(ValueError, lambda: meta_str.index('full_resource'))

    @patch('exporter.utils.to_dcp_version')
    def test_dcp_version_is_updated_on_init(self, to_dcp: MagicMock):
        # given
        to_dcp.return_value = 'IamADateTimeString'
        data = self._create_test_data(str(uuid.uuid4()))

        # when
        meta = MetadataResource.from_dict(data)

        # then
        to_dcp.assert_called_with(data.get('dcpVersion'))
        self.assertEqual(meta.dcp_version, to_dcp.return_value)

    @staticmethod
    def _create_test_data(uuid_value):
        return {'type': 'Biomaterial',
                'uuid': {'uuid': uuid_value},
                'content': {'describedBy': "http://some-schema/1.2.3",
                            'some': {'content': ['we', 'are', 'agnostic', 'of']}},
                'dcpVersion': '2019-12-02T13:40:50.520Z',
                'submissionDate': 'a date',
                'updateDate': 'another date'}


class MetadataServiceTest(TestCase):
    def test_fetch_resource(self):
        # given:
        ingest_client = Mock(name='ingest_client')
        uuid_value = str(uuid.uuid4())
        raw_metadata = {'type': 'Biomaterial',
                        'uuid': {'uuid': uuid_value},
                        'content': {'describedBy': "http://some-schema/1.2.3",
                                    'some': {'content': ['we', 'are', 'agnostic', 'of']}},
                        'dcpVersion': '2019-12-02T13:40:50.520Z',
                        'submissionDate': 'a submission date',
                        'updateDate': 'an update date'
                        }
        ingest_client.get_entity_by_callback_link = Mock(return_value=raw_metadata)

        # and:
        metadata_service = MetadataService(ingest_client)

        # when:
        metadata_resource = metadata_service.fetch_resource(
            'hca.domain.com/api/cellsuspensions/301636f7-f97b-4379-bf77-c5dcd9f17bcb')

        # then:
        self.assertEqual('biomaterial', metadata_resource.metadata_type)
        self.assertEqual(uuid_value, metadata_resource.uuid)
        self.assertEqual(raw_metadata['content'], metadata_resource.metadata_json)
        self.assertEqual(utils.to_dcp_version(raw_metadata['dcpVersion']), metadata_resource.dcp_version)
        self.assertEqual(raw_metadata['submissionDate'], metadata_resource.provenance.submission_date)
        self.assertEqual(raw_metadata['updateDate'], metadata_resource.provenance.update_date)


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
