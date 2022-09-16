from dataclasses import dataclass, field, InitVar


@dataclass
class SubmissionExportMessage:
    msg: InitVar[dict]
    job_id: str = field(init=False)
    submission_uuid: str = field(init=False)
    project_uuid: str = field(init=False)
    callback_link: str = field(init=False)
    context: dict = field(init=False)

    def __post_init__(self, msg: dict):
        self.job_id = msg.get('exportJobId')
        self.submission_uuid = msg.get('submissionUuid')
        self.project_uuid = msg.get('projectUuid')
        self.callback_link = msg.get('callbackLink')
        self.context = msg.get('context')
