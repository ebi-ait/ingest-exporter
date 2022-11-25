from dataclasses import dataclass, InitVar, field
from datetime import datetime
from enum import Enum
from typing import Dict, List

from hca_ingest.utils.date import parse_date_string


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
    submission_id: str = field(init=False)
    created_date: datetime = field(init=False)
    updated_date: datetime = field(init=False)
    export_state: ExportJobState = field(init=False)
    num_expected_assays: int = field(init=False)
    data_file_transfer: ExportContextState = field(init=False)
    spreadsheet_generation: ExportContextState = field(init=False)

    def __post_init__(self, job: dict):
        self.job_id = str(job.get("_links", {}).get("self", {}).get("href", '')).partition("/exportJobs/")[2]
        self.submission_id = str(job.get("_links", {}).get("submission", {}).get("href", '')).partition("/submissionEnvelopes/")[2]
        self.created_date = parse_date_string(job["createdDate"]) if 'createdDate' in job else datetime.min
        self.updated_date = parse_date_string(job["updatedDate"]) if 'updatedDate' in job else datetime.now()
        self.export_state = ExportJobState(str(job.get("status", "EXPORTING")).upper())
        self.num_expected_assays = int(job.get("context", {}).get("totalAssayCount", 0))
        self.data_file_transfer = ExportContextState(str(job.get("context", {}).get("dataFileTransfer", 'NOT_STARTED')).upper())
        self.spreadsheet_generation = ExportContextState(str(job.get("context", {}).get("spreadsheetGeneration", 'NOT_STARTED')).upper())
