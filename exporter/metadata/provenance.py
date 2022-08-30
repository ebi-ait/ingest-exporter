import re
from dataclasses import dataclass, field

from packaging import version

from exporter.metadata.exceptions import MetadataParseException

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


@dataclass
class MetadataProvenance:
    document_id: str
    submission_date: str
    update_date: str
    schema_major_version: int = field(default=None)
    schema_minor_version: int = field(default=None)

    @staticmethod
    def from_dict(data: dict):
        try:
            uuid = data['uuid']['uuid']
            submission_date = data['submissionDate']
            update_date = data['updateDate']

            # Populate the major and minor schema versions from the URL in the describedBy field
            schema_semver = re.findall(r'\d+\.\d+\.\d+', data["content"]["describedBy"])[0]
            concrete_type = data['content']['describedBy'].rsplit('/', 1)[-1]
            version_with_schema_fields = SCHEMA_VERSIONS_WITHOUT_SCHEMA_FIELDS.get(concrete_type)
            if MetadataProvenance.version_has_schema_fields(schema_semver, version_with_schema_fields):
                schema_major_version, schema_minor_version = [int(x) for x in schema_semver.split(".")][:2]
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
