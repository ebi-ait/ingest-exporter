import inspect
import logging
from datetime import datetime

from hca_ingest.utils.date import parse_date_string

DCP_VERSION_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def to_dcp_version(date_str: str):
    date = parse_date_string(date_str)
    return date.strftime(DCP_VERSION_FORMAT)


def exec_time(logger: logging.Logger, level: int):
    def _exec_time(func):
        def inner(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            end_time = datetime.now()
            exec_time_ms = (end_time - start_time).total_seconds() * 1000
            func_args = inspect.signature(func).bind(*args, **kwargs).arguments
            func_args.pop('self', None)
            func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, func_args.items()))
            logger.log(level, f'{func.__name__} ( {func_args_str} ) exec time is: {exec_time_ms}')
            return result
        return inner
    return _exec_time


def log_function_and_params(logger: logging.Logger, level: int = logging.INFO):
    def _log_function_and_params(func):
        def inner(*args, **kwargs):
            func_args = inspect.signature(func).bind(*args, **kwargs).arguments
            func_args.pop('self', None)
            func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, func_args.items()))
            logger.log(level, f'{func.__name__} ( {func_args_str} )')
            return func(*args, **kwargs)
        return inner
    return _log_function_and_params
