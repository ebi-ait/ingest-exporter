import re
from copy import deepcopy
from packaging import version
from typing import List, Dict, Optional
from dataclasses import dataclass

from ingest.api.ingestapi import IngestApi

from exporter import utils

# These are the versions which started using https://schema.humancellatlas.org/system/1.1.0/provenance
# which introduced the schema_major_version and schema_minor_version
# The versions are sourced from metadata-schema changelog
# https://github.com/HumanCellAtlas/metadata-schema/blob/master/changelog.md#systemprovenancejson---v110---2019-07-25
SCHEMA_VERSIONS_WITHOUT_SCHEMA_FIELDS = {
    'cell_suspension': '13.2.0',
    'protocol': '7.1.0',
    'differentiation_protocol': '2.2.0',
    'cell_suspension': '13.2.0',
    'dissociation_protocol': '6.2.0',
    'reference_file': '3.2.0',
    'organoid': '11.2.0',
    'process': '9.2.0',
    'analysis_file': '6.2.0',
    'ipsc_induction_protocol': '3.2.0',
    'analysis_protocol': '9.1.0',
    'sequence_file': '9.2.0',
    'aggregate_generation_protocol': '2.1.0',
    'enrichment_protocol': '3.1.0',
    'collection_protocol': '9.2.0',
    'sequencing_protocol': '10.1.0',
    'supplementary_file': '2.2.0',
    'imaged_specimen': '3.2.0',
    'donor_organism': '15.4.0',
    'imaging_preparation_protocol': '2.2.0',
    'image_file': '2.2.0',
    'project': '14.1.0',
    'analysis_process': '11.1.0',
    'cell_line': '14.4.0',
    'library_preparation_protocol': '6.2.0',
    'imaging_protocol': '11.2.0',
    'specimen_from_organism': '10.3.0'
}


class MetadataParseException(Exception):
    pass


class MetadataException(Exception):
    pass


class MetadataProvenance:
    def __init__(self, document_id: str, submission_date: str, update_date: str,
                 schema_major_version: int = None, schema_minor_version: int = None):
        self.document_id = document_id
        self.submission_date = submission_date
        self.update_date = update_date

        if schema_major_version:
            self.schema_major_version = schema_major_version

        if schema_minor_version:
            self.schema_minor_version = schema_minor_version

    def to_dict(self):
        return deepcopy(self.__dict__)


class MetadataResource:

    def __init__(self, metadata_type, metadata_json, uuid, dcp_version,
                 provenance: MetadataProvenance, full_resource: dict):
        self.metadata_json = metadata_json
        self.uuid = uuid
        self.dcp_version = utils.to_dcp_version(dcp_version)
        self.metadata_type = metadata_type  # TODO: use an enum type instead of string
        self.provenance = provenance
        self.full_resource = full_resource

    def get_content(self, with_provenance=False) -> Dict:
        content = deepcopy(self.full_resource["content"])
        if with_provenance:
            content["provenance"] = self.provenance.to_dict()
            return content
        else:
            return content

    @staticmethod
    def from_dict(data: dict):
        try:
            metadata_json = data['content']
            uuid = data['uuid']['uuid']
            dcp_version = data['dcpVersion']
            metadata_type = data['type'].lower()
            provenance = MetadataResource.provenance_from_dict(data)
            return MetadataResource(metadata_type, metadata_json, uuid, dcp_version, provenance, full_resource=data)
        except (KeyError, TypeError) as e:
            raise MetadataParseException(e)

    @staticmethod
    def provenance_from_dict(data: dict):
        try:
            uuid = data['uuid']['uuid']
            submission_date = data['submissionDate']
            update_date = data['updateDate']

            # Populate the major and minor schema versions from the URL in the describedBy field
            schema_semver = re.findall(r'\d+\.\d+\.\d+', data["content"]["describedBy"])[0]
            concrete_type = data['content']['describedBy'].rsplit('/', 1)[-1]
            version_with_schema_fields = SCHEMA_VERSIONS_WITHOUT_SCHEMA_FIELDS.get(concrete_type)
            if MetadataResource.version_has_schema_fields(schema_semver, version_with_schema_fields):
                schema_major_version = int(schema_semver.split(".")[0])
                schema_minor_version = int(schema_semver.split(".")[1])
                return MetadataProvenance(uuid, submission_date, update_date, schema_major_version,
                                          schema_minor_version)
            else:
                return MetadataProvenance(uuid, submission_date, update_date)
        except (KeyError, TypeError) as e:
            raise MetadataParseException(e)

    @staticmethod
    def version_has_schema_fields(schema_semver, version_with_schema_fields):
        return not version_with_schema_fields or version.parse(schema_semver) >= version.parse(
            version_with_schema_fields)

    def concrete_type(self) -> str:
        return self.metadata_json["describedBy"].rsplit('/', 1)[-1]


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


@dataclass
class FileChecksums:
    sha256: str
    crc32c: str
    sha1: str
    s3_etag: str

    @staticmethod
    def from_dict(data: Dict) -> 'FileChecksums':
        try:
            sha256 = data["sha256"]
            crc32c = data["crc32c"]
            sha1 = data["sha1"]
            s3_etag = data["s3_etag"]

            return FileChecksums(sha256, crc32c, sha1, s3_etag)
        except (KeyError, TypeError) as e:
            raise MetadataParseException(e)


@dataclass
class DataFile:
    uuid: str
    dcp_version: str
    file_name: str
    cloud_url: str
    content_type: str
    size: int
    checksums: FileChecksums

    def source_bucket(self) -> str:
        return self.cloud_url.split("//")[1].split("/")[0]

    def source_key(self) -> str:
        return self.cloud_url.split("//")[1].split("/", 1)[1]

    @staticmethod
    def from_file_metadata(file_metadata: MetadataResource) -> 'DataFile':
        if file_metadata.full_resource is not None:
            try:
                return DataFile(file_metadata.full_resource["dataFileUuid"],
                                file_metadata.dcp_version,
                                file_metadata.full_resource["fileName"],
                                file_metadata.full_resource["cloudUrl"],
                                file_metadata.full_resource["fileContentType"],
                                file_metadata.full_resource["size"],
                                FileChecksums.from_dict(file_metadata.full_resource["checksums"]))
            except (KeyError, TypeError) as e:
                raise MetadataParseException(e)
        else:
            raise MetadataParseException(f'Error: parsing DataFile from file MetadataResources requires non-empty'
                                         f'"full_resource" field. Metadata:\n\n {file_metadata.metadata_json}')
