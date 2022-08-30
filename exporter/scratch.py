from dataclasses import dataclass, field, asdict
from typing import List, Dict

from hca_ingest.api.ingestapi import IngestApi

from exporter import utils
from exporter.metadata.exceptions import MetadataParseException
from exporter.metadata.provenance import MetadataProvenance


@dataclass
class MetadataResource:
    metadata_type: str
    metadata_json: str
    uuid: str
    dcp_version: str
    provenance: MetadataProvenance
    full_resource: dict = field(repr=False)

    def __post_init__(self):
        self.dcp_version = utils.to_dcp_version(self.dcp_version)

    def get_content(self, with_provenance=False) -> Dict:
        content = asdict(self).get('full_resource').get('content')
        if with_provenance:
            content["provenance"] = asdict(self.provenance)
        return content

    @staticmethod
    def from_dict(data: dict):
        try:
            metadata_json = data['content']
            uuid = data['uuid']['uuid']
            dcp_version = data['dcpVersion']
            metadata_type = data['type'].lower()
            provenance = MetadataProvenance.from_dict(data)
            return MetadataResource(metadata_type, metadata_json, uuid, dcp_version, provenance, data)
        except (KeyError, TypeError) as e:
            raise MetadataParseException(e)

    def concrete_type(self) -> str:
        return self.metadata_json.get("describedBy").rsplit('/', 1)[-1]


class MetadataService:

    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def fetch_resource(self, resource_link: str) -> MetadataResource:
        raw_metadata = self.ingest_client.get_entity_by_callback_link(resource_link)
        return MetadataResource.from_dict(raw_metadata)

    def get_derived_by_processes(self, experiment_material: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(
            self.ingest_client.get_related_entities('derivedByProcesses', experiment_material.full_resource,
                                                    'processes'))

    def get_input_to_processes(self, experiment_material: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(
            self.ingest_client.get_related_entities('inputToProcesses', experiment_material.full_resource, 'processes'))

    def get_derived_biomaterials(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(
            self.ingest_client.get_related_entities('derivedBiomaterials', process.full_resource, 'biomaterials'))

    def get_derived_files(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(
            self.ingest_client.get_related_entities('derivedFiles', process.full_resource, 'files'))

    def get_input_biomaterials(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(
            self.ingest_client.get_related_entities('inputBiomaterials', process.full_resource, 'biomaterials'))

    def get_input_files(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(
            self.ingest_client.get_related_entities('inputFiles', process.full_resource, 'files'))

    def get_protocols(self, process: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(
            self.ingest_client.get_related_entities('protocols', process.full_resource, 'protocols'))

    def get_supplementary_files(self, metadata: MetadataResource) -> List[MetadataResource]:
        return MetadataService.parse_metadata_resources(
            self.ingest_client.get_related_entities('supplementaryFiles', metadata.full_resource, 'files'))

    @staticmethod
    def parse_metadata_resources(metadata_resources: List[Dict]) -> List[MetadataResource]:
        return [MetadataResource.from_dict(m) for m in metadata_resources]


