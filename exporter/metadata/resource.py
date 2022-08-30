from dataclasses import dataclass, field, asdict
from typing import Dict

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
