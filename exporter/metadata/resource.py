from dataclasses import dataclass, field, asdict
from typing import Dict

from hca_ingest.utils.date import parse_date_string

from exporter.metadata.exceptions import MetadataParseException
from exporter.metadata.provenance import MetadataProvenance

DCP_VERSION_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


@dataclass
class MetadataResource:
    metadata_type: str
    metadata_json: dict
    uuid: str
    dcp_version: str
    provenance: MetadataProvenance
    full_resource: dict = field(repr=False)

    def __post_init__(self):
        self.dcp_version = self.to_dcp_version(self.dcp_version)

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

    @staticmethod
    def to_dcp_version(date_str: str):
        date = parse_date_string(date_str)
        return date.strftime(DCP_VERSION_FORMAT)

    def concrete_type(self) -> str:
        return self.metadata_json.get("describedBy").rsplit('/', 1)[-1]
