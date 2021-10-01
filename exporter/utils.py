import inspect
import logging
from datetime import datetime

INGEST_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
INGEST_DATE_FORMAT_SHORT = "%Y-%m-%dT%H:%M:%SZ"
DCP_VERSION_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

_expected_formats = [INGEST_DATE_FORMAT, INGEST_DATE_FORMAT_SHORT]

def to_dcp_version(date_str: str):
    date = parse_date_string(date_str)
    return date.strftime(DCP_VERSION_FORMAT)


def parse_date_string(date_str: str):
    for date_format in _expected_formats:
        try:
            return datetime.strptime(date_str, date_format)
        except ValueError:
            pass
    raise ValueError(f'unknown date format for [{date_str}]')


def exec_time(logger: logging.Logger, level: int):
    def _exec_time(func):
        def inner(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            end_time = datetime.now()
            exec_time_ms = (end_time - start_time).total_seconds() * 1000
            func_args = inspect.signature(func).bind(*args, **kwargs).arguments
            func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, func_args.items()))
            logger.log(level, f'{func.__module__}.{func.__qualname__} ( {func_args_str} ) exec time is: {exec_time_ms}')
            return result
        return inner
    return _exec_time


