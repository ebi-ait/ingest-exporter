from dataclasses import dataclass
from typing import List

from exporter.metadata.resource import MetadataResource


@dataclass
class ProcessInfo:
    process: MetadataResource
    inputs: List[MetadataResource]
    outputs: List[MetadataResource]
    protocols: List[MetadataResource]
