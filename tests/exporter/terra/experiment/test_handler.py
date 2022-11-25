import uuid
import random
from unittest.mock import Mock

import pytest
from kombu import Message

from exporter.ingest.service import IngestService
from exporter.queue.config import QueueConfig
from exporter.terra.experiment.exporter import TerraExperimentExporter
from exporter.terra.experiment.handler import TerraExperimentHandler


@pytest.fixture
def ingest():
    return Mock(spec=IngestService)


@pytest.fixture
def exporter():
    return Mock(spec=TerraExperimentExporter)


@pytest.fixture
def queue():
    return Mock(spec=QueueConfig)


@pytest.fixture
def handler(ingest, exporter, queue):
    ingest.job_exists_with_submission.return_value = True
    return TerraExperimentHandler(exporter, ingest, queue)


@pytest.fixture
def missing_job_handler(ingest, exporter, queue):
    ingest.job_exists_with_submission.return_value = False
    return TerraExperimentHandler(exporter, ingest, queue)


@pytest.fixture
def message():
    return Mock(spec=Message)


@pytest.fixture
def export_job_id() -> str:
    return str(uuid.uuid4()).replace('-', '')


@pytest.fixture
def process_id() -> str:
    return str(uuid.uuid4()).replace('-', '')


@pytest.fixture
def submission_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def process_uuid() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def total_processes() -> int:
    return random.randint(1, 99999)


@pytest.fixture
def current_process(total_processes) -> int:
    return random.randint(1, total_processes)


@pytest.fixture
def body(process_id, process_uuid, submission_uuid, current_process, total_processes, export_job_id) -> dict:
    return {
        'documentId': process_id,
        'documentUuid': process_uuid,
        'envelopeUuid': submission_uuid,
        'index': current_process,
        'total': total_processes,
        'exportJobId': export_job_id,
    }


def test_happy_path(handler, body, message, ingest, exporter, queue, process_uuid, export_job_id, process_id):
    # When
    handler.handle_message(body, message)
    # Then
    exporter.export.assert_called_once_with(process_uuid)
    ingest.create_export_entity.assert_called_once_with(export_job_id, process_id)
    queue.send_message.assert_called_once_with(handler.producer, body)
    message.ack.assert_called_once()


def test_missing_job_or_submission(missing_job_handler, body, message, ingest, exporter, queue):
    # When
    missing_job_handler.handle_message(body, message)

    # Then
    message.ack.assert_called_once()

    exporter.export.assert_not_called()
    ingest.create_export_entity.assert_not_called()
    queue.send_message.assert_not_called()
