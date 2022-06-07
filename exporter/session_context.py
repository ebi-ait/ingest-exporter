import logging
import threading
from typing import List

LOG_FORMAT_WITH_CONTEXT = '%(asctime)s - %(name)s - %(levelname)s - submission_uuid:%(submission_uuid)s - export_job_id:%(export_job_id)s - %(message)s'

'''
see https://stackoverflow.com/questions/6618513/python-logging-with-context
'''

log_context_data = threading.local()


def configure_logger(logger):
    handler = logging.StreamHandler()
    format_log = LOG_FORMAT_WITH_CONTEXT
    handler.setFormatter(logging.Formatter(format_log))
    handler.addFilter(SessionContextFilter(attributes=['submission_uuid', 'export_job_id']))
    logger.addHandler(handler)


class SessionContextFilter(logging.Filter):
    def __init__(self, attributes: List[str]):
        super().__init__()
        self.attributes = attributes

    def filter(self, record):
        for attribute in self.attributes:
            setattr(record, attribute, getattr(log_context_data, attribute, 'n/a'))
        return True


class SessionContext(object):
    def __init__(self, logger, context: dict = None):
        self.logger = logger
        self.context: dict = context

    def __enter__(self):
        for key, val in self.context.items():
            setattr(log_context_data, key, val)
        return self

    def __exit__(self, et, ev, tb):
        for key in self.context.keys():
            delattr(log_context_data, key)