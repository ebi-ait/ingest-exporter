from typing import List, Dict

from hca_ingest.api.ingestapi import IngestApi

from exporter.metadata.resource import MetadataResource


class MetadataService:

    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client

    def fetch_resource(self, resource_link: str) -> MetadataResource:
        raw_metadata = self.ingest_client.get_entity_by_callback_link(resource_link)
        return MetadataResource.from_dict(raw_metadata)

    def get_derived_by_processes(self, experiment_material: MetadataResource) -> List[
        MetadataResource]:
        return MetadataService.parse_metadata_resources(
            self.ingest_client.get_related_entities('derivedByProcesses', experiment_material.full_resource,
                                                    'processes'))

    def get_input_to_processes(self, experiment_material: MetadataResource) -> List[
        MetadataResource]:
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


