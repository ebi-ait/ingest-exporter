import logging
from datetime import datetime
from unittest import TestCase
from testfixtures import log_capture
from unittest.mock import patch

from exporter import utils


class UtilsTest(TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger('test_logger')
        handler = logging.StreamHandler()
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def test_parse_date_string__returns_correct_date_obj__given_iso(self):
        # given:
        expected_date = datetime(year=2019, month=6, day=12, hour=9, minute=49, second=25)
        # and:
        iso_date = '2019-06-12T09:49:25.000Z'

        # when:
        actual_date = utils.parse_date_string(iso_date)

        # expect:
        self.assertEqual(expected_date, actual_date)

    def test_parse_date_string__returns_correct_date_obj__given_iso_short(self):
        # given:
        iso_date_short = '2019-06-12T09:49:25Z'

        # when:
        actual_date = utils.parse_date_string(iso_date_short)

        # expect:
        expected_date = datetime(year=2019, month=6, day=12, hour=9, minute=49, second=25)
        self.assertEqual(expected_date, actual_date)

    def test_parse_date_string__returns_correct_date_obj__given_unknown_format(self):
        # given:
        unknown = '2019:06:12Y09-49-25.000X'

        # expect:
        with self.assertRaises(ValueError):
            utils.parse_date_string(unknown)

    def test_to_dcp_version__returns_correct_dcp_format__given_short_date(self):
        # given:
        date_string = '2019-05-23T16:53:40Z'

        # expect:
        self.assertEqual('2019-05-23T16:53:40.000000Z', utils.to_dcp_version(date_string))

    def test_to_dcp_version__returns_correct_dcp_format__given_3_decimal_places(self):
        # given:
        date_string = '2019-05-23T16:53:40.931Z'

        # expect:
        self.assertEqual('2019-05-23T16:53:40.931000Z', utils.to_dcp_version(date_string))

    def test_to_dcp_version__returns_correct_dcp_format__given_2_decimal_places(self):
        # given:
        date_string = '2019-05-23T16:53:40.93Z'

        # expect:
        self.assertEqual('2019-05-23T16:53:40.930000Z', utils.to_dcp_version(date_string))

    def test_to_dcp_version__returns_correct_dcp_format__given_6_decimal_places(self):
        # given:
        date_string = '2019-05-23T16:53:40.123456Z'

        # expect:
        self.assertEqual(date_string, utils.to_dcp_version(date_string))

    @log_capture()
    @patch('exporter.utils.datetime')
    def test_log_exec_time__logs_exec_time(self, mock_datetime, capture):
        start_time = datetime(2021, 10, 1, 12, 00)
        min_after = 5
        end_time = datetime(2021, 10, 1, 12, min_after)
        mock_datetime.now.side_effect = [start_time, end_time]
        elapsed_time = (end_time - start_time).total_seconds() * 1000

        @utils.exec_time(self.logger, logging.INFO)
        def hello(name: str):
            print(f'hello {name}')

        hello('name')

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
