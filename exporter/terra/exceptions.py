class DcpStagingException(Exception):
    pass


class UploadPollingException(Exception):
    pass


class ExperimentMessageParseException(Exception):
    pass


class SubmissionDoesNotHaveRequiredAction(Exception):
    pass


class SubmissionDoesNotHaveStagingArea(Exception):
    pass
