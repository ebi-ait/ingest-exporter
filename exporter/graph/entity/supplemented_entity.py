from dataclasses import dataclass
from typing import Dict


@dataclass
class SupplementedEntity:
    entity_type: str
    entity_id: str

    def to_dict(self) -> Dict[str, str]:
        return dict(
            entity_type=self.entity_type,
            entity_id=self.entity_id
        )
