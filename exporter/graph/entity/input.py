from dataclasses import dataclass
from typing import Dict


@dataclass
class Input:
    input_type: str
    input_uuid: str

    def to_dict(self) -> Dict[str, str]:
        return dict(
            input_type=self.input_type,
            input_id=self.input_uuid
        )
