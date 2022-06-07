import logging
import unittest
from threading import Thread
from unittest.mock import patch, Mock

from exporter.session_context import SessionContext, configure_logger


class TestLoggingTestCase(unittest.TestCase):

    def set_value(self, value):
        with SessionContext(logger=self.logger,
                            context={'submission_uuid': value}):
            self.logger.info(f'logging from child')

    # @patch.object(logging.StreamHandler, 'emit', wraps=logging.StreamHandler.emit)
    def test_child_different_than_current_thread(self):
        self.logger = logging.getLogger(__name__)
        configure_logger(self.logger)
        self.logger.setLevel(logging.INFO)
        with SessionContext(logger=self.logger,
                            context={'submission_uuid': 'parent_thread'}):
            self.logger.info(f'logging from parent')
            th1 = Thread(target=self.set_value, args=('child_thread',))
            th1.start()
            th1.join()
            self.logger.info(f'logging from parent after child')

            # self.assertEquals(mock_log_handler.call_count, 3, 'expecting 3 calls')
            # log_record = mock_log_handler.call_args_list[0].args[0]
            # self.assertEquals(log_record.submission_uuid, 'parent_thread')
            # log_record = mock_log_handler.call_args_list[1].args[0]
            # self.assertEquals(log_record.submission_uuid, 'child')
            # log_record = mock_log_handler.call_args_list[2].args[0]
            # self.assertEquals(log_record.submission_uuid, 'parent_thread')

