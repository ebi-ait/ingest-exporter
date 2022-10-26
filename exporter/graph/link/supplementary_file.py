from dataclasses import dataclass
from typing import Iterable, Dict, Any

from exporter.graph.entity.supplementary_file import SupplementaryFile
from exporter.graph.entity.supplemented_entity import SupplementedEntity


@dataclass
class SupplementaryFileLink:
    supplemented_entity: SupplementedEntity
    files: Iterable[SupplementaryFile]

    def to_dict(self) -> Dict[str, Any]:
        return dict(
            link_type="supplementary_file_link",
            entity=self.supplemented_entity.to_dict(),
            files=[file.to_dict() for file in self.files]
        )
