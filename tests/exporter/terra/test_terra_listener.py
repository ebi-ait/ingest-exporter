from unittest import TestCase

from concurrent.futures import ThreadPoolExecutor
from kombu import Connection, Message
from mock import MagicMock

from exporter.amqp import QueueConfig
from exporter.terra.terra_listener import _TerraListener
from exporter.terra.terra_exporter import TerraExporter, TerraExportJobService


class TerraMessageHandlerTest(TestCase):
    def setUp(self) -> None:
        self.publish_config = {
            'exchange': 'exchange',
            'routing_key': 'routing_key',
            'retry_policy': {}
        }
        self.exporter_mock = MagicMock(spec=TerraExporter)
        self.job_service_mock = MagicMock(spec=TerraExportJobService)
        self.listener = _TerraListener(
            connection=MagicMock(spec=Connection),
            terra_exporter=self.exporter_mock,
            job_service=self.job_service_mock,
            experiment_queue_config=MagicMock(spec=QueueConfig),
            publish_queue_config=QueueConfig('exchange', 'routing_key'),
            executor=MagicMock(spec=ThreadPoolExecutor)
        )
        self.exporter_mock.export = MagicMock()
        self.job_service_mock.create_export_entity = MagicMock()

    def test_success(self):
        # Given
        body = '{"documentId": "D", "documentUuid": "P", "envelopeUuid": "S", "index": 0, "total": 1, "exportJobId": "E"}'
        message = MagicMock(spec=Message)
        message.ack = MagicMock()

        # When
        self.listener._experiment_message_handler(body, message)

        # Then
        self.exporter_mock.export.assert_called_with("P", "S", "E")
        self.job_service_mock.create_export_entity.assert_called_with("E", "D")
        message.ack.assert_called_once()

    def test_failure(self):
        # Given
        body = '{"documentId": "D", "documentUuid": "P", "envelopeUuid": "S", "index": 0, "total": 1, "exportJobId": "E"}'
        message = MagicMock(spec=Message)
        message.reject = MagicMock()
        self.exporter_mock.export.side_effect = Exception('unhandled exception')

        # When
        self.listener._experiment_message_handler(body, message)

        # Then
        self.exporter_mock.export.assert_called_with("P", "S", "E")
        message.reject.assert_called_once_with(requeue=False)
