import json
from dataclasses import dataclass, field, InitVar


@dataclass
class SubmissionMessage:
    original_message: InitVar[str]
    job_id: str = field(init=False)
    submission_uuid: str = field(init=False)
    project_uuid: str = field(init=False)
    callback_link: str = field(init=False)
    context: dict = field(init=False)

    def __post_init__(self, original_message):
        original_json = json.loads(original_message)
        self.job_id = original_json.get('exportJobId')
        self.submission_uuid = original_json.get('submissionUuid')
        self.project_uuid = original_json.get('projectUuid')
        self.callback_link = original_json.get('callbackLink')
        self.context = original_json.get('context')
