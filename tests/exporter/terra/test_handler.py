import json
from unittest import TestCase

from kombu import Message
from mock import MagicMock

from exporter.ingest.service import IngestService
from exporter.queue.config import QueueConfig
from exporter.queue.listener import QueueListener
from exporter.terra.experiment.exporter import TerraExperimentExporter
from exporter.terra.experiment.handler import TerraExperimentHandler


class TerraMessageHandlerTest(TestCase):
    def setUp(self) -> None:
        self.publish_config = {
            'exchange': 'exchange',
            'routing_key': 'routing_key',
            'retry_policy': {}
        }
        self.exporter_mock = MagicMock(spec=TerraExperimentExporter)
        self.ingest_service_mock = MagicMock(spec=IngestService)
        self.producer = MagicMock()
        self.producer.publish = MagicMock()
        self.handler = TerraExperimentHandler(
            self.exporter_mock,
            self.ingest_service_mock,
            QueueConfig('exchange', 'routing_key')
        )
        self.handler.add_producer(self.producer)

        self.exporter_mock.export = MagicMock()
        self.ingest_service_mock.create_export_entity = MagicMock()

    def test_success(self):
        # Given
        body = json.loads('{"documentId": "D", "documentUuid": "P", "envelopeUuid": "S", "index": 0, "total": 1, "exportJobId": "E"}')
        message = MagicMock(spec=Message)
        message.ack = MagicMock()

        # When
        self.handler.handle_message(body, message)

        # Then
        self.exporter_mock.export.assert_called_with("P")
        self.ingest_service_mock.create_export_entity.assert_called_with("E", "D")
        message.ack.assert_called_once()
        self.producer.publish.assert_called_once()

    def test_listener_failure(self):
        # Given
        body = '{"documentId": "D", "documentUuid": "P", "envelopeUuid": "S", "index": 0, "total": 1, "exportJobId": "E"}'
        message = MagicMock(spec=Message)
        message.reject = MagicMock()
        self.exporter_mock.export.side_effect = Exception('unhandled exception')
        listener = QueueListener(MagicMock(), self.handler)

        # When
        listener.try_handle_or_reject(body, message)

        # Then
        self.exporter_mock.export.assert_called_with("P")
        message.reject.assert_called_once_with(requeue=False)
