import logging

import pytest
import json
from unittest.mock import MagicMock, Mock

from kombu import Message

from exporter.queue.handler import MessageHandler
from exporter.queue.listener import QueueListener
from exporter.session_context import SessionContext


class MockHandler(MessageHandler):
    def set_context(self, body: dict) -> SessionContext:
        return SessionContext(
            logger=logging.getLogger(),
            context={'thing': body.get('thing')}
        )


class FailingHandler(MockHandler):
    def handle_message(self, body: dict, msg: Message):
        raise Exception('unhandled exception')


@pytest.fixture
def handler():
    return MockHandler(__name__)


@pytest.fixture
def handler_with_error(handler):
    return FailingHandler(__name__)


@pytest.fixture
def message():
    return Mock(spec=Message)


@pytest.fixture
def body() -> str:
    return '{}'


@pytest.fixture
def json_body(body: str) -> dict:
    return json.loads(body)


def test_failure(handler_with_error, body: str, message, json_body):
    # Given
    listener = QueueListener(MagicMock(), handler_with_error)
    # When
    listener.try_handle_or_reject(body, message)
    # Then
    message.reject.assert_called_once_with(requeue=False)
