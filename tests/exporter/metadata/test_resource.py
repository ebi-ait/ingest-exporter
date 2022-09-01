import uuid
from dataclasses import asdict
from unittest import TestCase
from unittest.mock import patch, MagicMock

from exporter.metadata.exceptions import MetadataParseException
from exporter.metadata.provenance import MetadataProvenance
from exporter.metadata.resource import MetadataResource


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
        self.assertEqual(MetadataResource.to_dcp_version(data['dcpVersion']), metadata.dcp_version)

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

    @patch('exporter.metadata.resource.MetadataResource.to_dcp_version')
    def test_dcp_version_is_updated_on_init(self, to_dcp: MagicMock):
        # given
        to_dcp.return_value = 'IamADateTimeString'
        data = self._create_test_data(str(uuid.uuid4()))

        # when
        meta = MetadataResource.from_dict(data)

        # then
        to_dcp.assert_called_with(data.get('dcpVersion'))
        self.assertEqual(meta.dcp_version, to_dcp.return_value)

    def test_to_dcp_version__returns_correct_dcp_format__given_short_date(self):
        # given:
        date_string = '2019-05-23T16:53:40Z'

        # expect:
        self.assertEqual('2019-05-23T16:53:40.000000Z', MetadataResource.to_dcp_version(date_string))

    def test_to_dcp_version__returns_correct_dcp_format__given_3_decimal_places(self):
        # given:
        date_string = '2019-05-23T16:53:40.931Z'

        # expect:
        self.assertEqual('2019-05-23T16:53:40.931000Z', MetadataResource.to_dcp_version(date_string))

    def test_to_dcp_version__returns_correct_dcp_format__given_2_decimal_places(self):
        # given:
        date_string = '2019-05-23T16:53:40.93Z'

        # expect:
        self.assertEqual('2019-05-23T16:53:40.930000Z', MetadataResource.to_dcp_version(date_string))

    def test_to_dcp_version__returns_correct_dcp_format__given_6_decimal_places(self):
        # given:
        date_string = '2019-05-23T16:53:40.123456Z'

        # expect:
        self.assertEqual(date_string, MetadataResource.to_dcp_version(date_string))

    @staticmethod
    def _create_test_data(uuid_value):
        return {'type': 'Biomaterial',
                'uuid': {'uuid': uuid_value},
                'content': {'describedBy': "http://some-schema/1.2.3",
                            'some': {'content': ['we', 'are', 'agnostic', 'of']}},
                'dcpVersion': '2019-12-02T13:40:50.520Z',
                'submissionDate': 'a date',
                'updateDate': 'another date'}