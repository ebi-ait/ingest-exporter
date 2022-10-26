from dataclasses import dataclass, InitVar, field
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


class ExportContextState(Enum):
    NOT_STARTED = "NOT_STARTED"
    STARTED = "STARTED"
    COMPLETE = "COMPLETE"


@dataclass
class ExportJob:
    job: InitVar[dict]
    job_id: str = field(init=False)
    num_expected_assays: int = field(init=False, default=0)
    export_state: ExportJobState = field(init=False)
    data_file_transfer: ExportContextState = field(init=False, default=ExportContextState.NOT_STARTED)
    spreadsheet_generation: ExportContextState = field(init=False, default=ExportContextState.NOT_STARTED)

    def __post_init__(self, job: dict):
        self.job_id = str(job["_links"]["self"]["href"]).split("/")[-1]
        self.export_state = ExportJobState(job["status"].upper())
        if 'context' in job:
            if 'totalAssayCount' in job['context']:
                self.num_expected_assays = int(job["context"]["totalAssayCount"])
            if 'dataFileTransfer' in job["context"]:
                self.data_file_transfer = ExportContextState(job["context"]["dataFileTransfer"].upper())
            if 'spreadsheetGeneration' in job["context"]:
                self.spreadsheet_generation = ExportContextState(job["context"]["spreadsheetGeneration"].upper())
