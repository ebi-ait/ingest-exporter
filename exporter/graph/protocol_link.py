from dataclasses import dataclass
from typing import Dict

from exporter.metadata.resource import MetadataResource


@dataclass
class ProtocolLink:
    protocol_type: str
    protocol_uuid: str

    @staticmethod
    def from_metadata_resource(metadata: MetadataResource) -> 'ProtocolLink':
        return ProtocolLink(metadata.concrete_type(), metadata.uuid)

    def to_dict(self) -> Dict:
        return dict(
            protocol_type=self.protocol_type,
            protocol_id=self.protocol_uuid
        )
