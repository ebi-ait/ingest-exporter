import logging
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch

from testfixtures import log_capture

from exporter import utils


class UtilsTest(TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger('test_logger')
        handler = logging.StreamHandler()
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    @utils.exec_time(logging.getLogger('test_logger'), logging.INFO)
    def hello(self, name: str):
        print(f'hello {name}')

    @log_capture
    @patch('exporter.utils.datetime')
    def test_log_exec_time__logs_exec_time(self, mock_datetime, capture):
        start_time = datetime(2021, 10, 1, 12, 00)
        min_after = 5
        end_time = datetime(2021, 10, 1, 12, min_after)
        mock_datetime.now.side_effect = [start_time, end_time]
        elapsed_time = (end_time - start_time).total_seconds() * 1000

        self.hello('name')

        capture.check((
            'test_logger',
            'INFO',
            f"hello ( name = 'name' ) exec time is: {elapsed_time}"
        ))

    def test_log_exec_time__return_wrapped_func_output(self):
        @utils.exec_time(self.logger, logging.INFO)
        def echo(message: str):
            return message

        message = echo('message')

        self.assertEqual(message, 'message')
