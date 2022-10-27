from unittest.mock import Mock, ANY

import pytest
from assertpy import assert_that, extracting
from hca_ingest.api.ingestapi import IngestApi
from openpyxl.workbook import Workbook

from exporter.ingest.service import IngestService
from exporter.terra.spreadsheet.exporter import SpreadsheetExporter
from exporter.terra.storage import TerraStorageClient


@pytest.fixture
def ingest_service(mocker, ingest_api):
    ingest_service: IngestService = IngestService(ingest_api)
    return ingest_service


@pytest.fixture
def ingest_api(mocker, submission, project):
    ingest_api: IngestApi = mocker.Mock(spec=IngestApi)
    ingest_api.get_submission_by_uuid.return_value = submission
    ingest_api.get_latest_schema_url.return_value = 'https://schema.humancellatlas.org/type/file/2.5.0/supplementary_file'
    ingest_api.get_entity_by_uuid.return_value = project
    return ingest_api


@pytest.fixture
def project():
    return {
        "uuid": { "uuid": "project-uuid" },
        "dcpVersion": "2022-05-29T13:51:08.593000Z",
        "content": {
            "describedBy": "https://schema.humancellatlas.org/type/project/17.0.0/project",
        },
        "type": "file",
        "submissionDate": "2022-03-28T13:51:08.593000Z",
        "updateDate": "2022-05-28T13:51:08.593000Z",
    }


@pytest.fixture
def submission():
    return {
        "uuid": {"uuid": 'submission-uuid'},
        "_links": {
            "self": {
                "href": "http://ingest/submissionEnvelopes/submission-id"
            }
        }
    }


@pytest.fixture
def terra_client(mocker):
    terra_client: TerraStorageClient = Mock(spec=TerraStorageClient)
    return terra_client


def test_export(mocker,
                ingest_service: Mock,
                terra_client: Mock,
                project,
                submission,
                caplog):
    # given
    exporter = SpreadsheetExporter(ingest_service, terra_client)
    exporter.downloader.get_workbook_from_submission = mocker.Mock(return_value=Workbook())

    # when
    exporter.export_spreadsheet(job_id='test_job_id',
                                project_uuid=project['uuid']['uuid'],
                                submission_uuid=submission['uuid']['uuid'])

    # then
    actual_file_metadata = check_file_metadata(project, terra_client)
    check_generated_links(actual_file_metadata, project, terra_client)
    check_spreadsheet_copied_to_terra(actual_file_metadata, project, terra_client)
    assert "Generating Spreadsheet" in caplog.text


def check_spreadsheet_copied_to_terra(actual_file_metadata, project, terra_client):
    terra_client.write_to_staging_bucket.assert_called_with(
        object_key=f'{project["uuid"]["uuid"]}/data/{actual_file_metadata.full_resource["fileName"]}',
        data_stream=ANY
    )


def check_generated_links(actual_file_metadata, project, terra_client):
    terra_client.write_links.assert_called_with(ANY,
                                                actual_file_metadata.uuid,
                                                project['dcpVersion'],
                                                project['uuid']['uuid'])


def check_file_metadata(project, terra_client):
    terra_client.write_metadata.assert_called_with(ANY, project['uuid']['uuid'])
    actual_file_metadata = terra_client.write_metadata.call_args.args[0]
    assert_that(actual_file_metadata.metadata_json['file_core']) \
        .has_format('xlsx') \
        .has_file_source("DCP/2 Ingest") \
        .has_content_description([
        {
            "text": "metadata spreadsheet",
            "ontology": "data:2193",
            "ontology_label": "Database entry metadata"
        }
    ])
    return actual_file_metadata
