from dataclasses import dataclass
from enum import Enum
from typing import Dict, List


@dataclass
class ExportError:
    message: str

    def to_dict(self) -> Dict:
        return {
            "message": self.message,
            "errorCode": -1,
            "details": {}
        }


@dataclass
class ExportEntity:
    assay_process_id: str
    errors: List[ExportError]

    def to_dict(self) -> Dict:
        """
        converts to a JSON as represented in ingest-core API
        """
        return {
            "status": ExportJobState.EXPORTED.value,
            "context": {
                "assayProcessId": self.assay_process_id
            },
            "errors": [e.to_dict() for e in self.errors]
        }


class ExportJobState(Enum):
    EXPORTING = "EXPORTING"
    EXPORTED = "EXPORTED"
    DEPRECATED = "DEPRECATED"
    FAILED = "FAILED"


@dataclass
class ExportJob:
    job_id: str
    num_expected_assays: int
    export_state: ExportJobState
    is_data_transfer_complete: bool

    @staticmethod
    def from_dict(data: Dict) -> 'ExportJob':
        job_id = str(data["_links"]["self"]["href"]).split("/")[0]
        num_expected_assays = int(data["context"]["totalAssayCount"])
        is_data_transfer_complete = data["context"].get("isDataTransferComplete")
        return ExportJob(job_id, num_expected_assays, ExportJobState(data["status"].upper()),
                         is_data_transfer_complete)
