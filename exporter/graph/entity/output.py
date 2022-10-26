from dataclasses import dataclass
from typing import Dict


@dataclass
class Output:
    output_type: str
    output_uuid: str

    def to_dict(self) -> Dict[str, str]:
        return dict(
            output_type=self.output_type,
            output_id=self.output_uuid
        )
