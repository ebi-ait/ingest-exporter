from dataclasses import dataclass
from typing import List

from exporter.metadata.resource import MetadataResource


@dataclass
class SupplementaryFilesInfo:
    for_entity: MetadataResource
    files: List[MetadataResource]
