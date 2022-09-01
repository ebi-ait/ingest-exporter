from dataclasses import dataclass
from typing import Dict


@dataclass
class SupplementaryFile:
    file_type: str
    file_id: str

    def to_dict(self) -> Dict[str, str]:
        return dict(
            file_type=self.file_type,
            file_id=self.file_id
        )
