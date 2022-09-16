import uuid
from unittest import TestCase
from unittest.mock import Mock

from exporter.metadata.resource import MetadataResource
from exporter.metadata.service import MetadataService


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
        self.assertEqual(MetadataResource.to_dcp_version(raw_metadata['dcpVersion']), metadata_resource.dcp_version)
        self.assertEqual(raw_metadata['submissionDate'], metadata_resource.provenance.submission_date)
        self.assertEqual(raw_metadata['updateDate'], metadata_resource.provenance.update_date)
