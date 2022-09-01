from dataclasses import dataclass
from typing import Dict

from exporter.terra.exceptions import ExperimentMessageParseException


@dataclass
class ExperimentMessage:
    process_id: str
    process_uuid: str
    submission_uuid: str
    experiment_index: int
    total: int
    job_id: str

    @staticmethod
    def from_dict(data: Dict) -> 'ExperimentMessage':
        try:
            return ExperimentMessage(data["documentId"],
                                     data["documentUuid"],
                                     data["envelopeUuid"],
                                     data["index"],
                                     data["total"],
                                     data["exportJobId"])
        except (KeyError, TypeError) as e:
            raise ExperimentMessageParseException(e)

    @staticmethod
    def as_dict(exp: 'ExperimentMessage') -> Dict:
        try:
            return {
                "documentId": exp.process_id,
                "documentUuid": exp.process_uuid,
                "envelopeUuid": exp.submission_uuid,
                "index": exp.experiment_index,
                "total": exp.total,
                "exportJobId": exp.job_id
            }
        except (KeyError, TypeError) as e:
            raise ExperimentMessageParseException(e)
