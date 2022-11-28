import logging
import threading
from typing import List

LOG_PREFIX = '%(asctime)s - %(name)s - %(levelname)s'
LOG_SUFFIX = '%(message)s'
LOG_FORMAT = f'{LOG_PREFIX} - {LOG_SUFFIX}'
'''
see https://stackoverflow.com/questions/6618513/python-logging-with-context
'''

log_context_data = threading.local()
BASIC_HANDLER = logging.StreamHandler()
BASIC_HANDLER.setFormatter(logging.Formatter(LOG_FORMAT))


class SessionContextFilter(logging.Filter):
    def __init__(self, attributes: List[str]):
        super().__init__()
        self.attributes = attributes

    def filter(self, record):
        for attribute in self.attributes:
            setattr(record, attribute, get_session_value(attribute))
        return True


def get_session_value(key: str):
    return getattr(log_context_data, key, 'n/a')


class SessionContext(object):
    def __init__(self, logger: logging.Logger, context: dict = None):
        self.logger = logger
        self.handler = None
        self.filter = None
        self.context: dict = context if context else {}

    def __enter__(self):
        log_format = self.make_log_format(self.context)
        self.handler = logging.StreamHandler()
        self.handler.setFormatter(logging.Formatter(f'{LOG_PREFIX} - {log_format} - {LOG_SUFFIX}'))
        self.filter = SessionContextFilter(attributes=list(self.context.keys()))
        self.handler.addFilter(self.filter)
        self.logger.removeHandler(BASIC_HANDLER)
        self.logger.addHandler(self.handler)

        for key, val in self.context.items():
            setattr(log_context_data, key, val)
        return self

    def __exit__(self, et, ev, tb):
        self.logger.removeHandler(self.handler)
        self.logger.addHandler(BASIC_HANDLER)
        for key in self.context.keys():
            delattr(log_context_data, key)

    @staticmethod
    def register_logger(logger_name: str) -> logging.Logger:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(BASIC_HANDLER)
        logger.propagate = False
        return logger

    @staticmethod
    def make_log_format(context: dict):
        key_names = list(context.keys())
        key_formats = []
        for key in key_names:
            key_formats.append(f'{key}:%({key})s')
        return ' - '.join(key_formats)
