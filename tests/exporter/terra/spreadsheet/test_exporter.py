from tempfile import NamedTemporaryFile
from unittest.mock import Mock, ANY

import pytest
from assertpy import assert_that
from hca_ingest.api.ingestapi import IngestApi
from openpyxl.workbook import Workbook

from exporter.ingest.service import IngestService
from exporter.metadata.descriptor import FileDescriptor
from exporter.metadata.resource import MetadataResource
from exporter.schema.resource import SchemaResource
from exporter.terra.exceptions import SpreadsheetExportError
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
        "uuid": {"uuid": "project-uuid"},
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
    terra_client: TerraStorageClient = mocker.Mock(spec=TerraStorageClient)
    return terra_client


@pytest.fixture()
def workbook():
    return Workbook()


@pytest.fixture
def exporter(ingest_service, terra_client, workbook, mocker):
    exporter = SpreadsheetExporter(ingest_service, terra_client)
    exporter.downloader.get_workbook_from_submission = mocker.Mock(return_value=workbook)
    return exporter


@pytest.fixture()
def failing_exporter(ingest_service, terra_client, mocker):
    exporter = SpreadsheetExporter(ingest_service, terra_client)
    exporter.downloader.get_workbook_from_submission = mocker.Mock(side_effect=RuntimeError('spreadsheet generation problem'))
    return exporter


def test_happy_path(exporter: SpreadsheetExporter,
                    ingest_service: Mock,
                    terra_client: Mock,
                    project,
                    submission,
                    caplog):
    # given
    # uses an exporter fixture

    # when
    exporter.export_spreadsheet(job_id='test_job_id',
                                project_uuid=project['uuid']['uuid'],
                                submission_uuid=submission['uuid']['uuid'])

    # then
    actual_file_metadata = check_file_metadata(project, terra_client)
    check_generated_links(actual_file_metadata, project, terra_client)
    check_spreadsheet_copied_to_terra(actual_file_metadata, project, terra_client)
    assert "Generating Spreadsheet" in caplog.text


def test_exception_during_export(failing_exporter: SpreadsheetExporter,
                                 caplog):
    # given an exception is thrown while generating the spreadsheet

    # when
    with pytest.raises(SpreadsheetExportError):
        project_uuid = 'test-project-uuid'
        failing_exporter.export_spreadsheet(job_id='test_job_id',
                                            project_uuid=project_uuid,
                                            submission_uuid='test-submission-uuid')
    assert_that(caplog.text) \
        .contains("problem generating spreadsheet") \
        .contains(project_uuid)


def test_spreadsheet_metadata_entity(exporter, project, workbook, terra_client):
    with NamedTemporaryFile() as spreadsheet_file:
        workbook.save(spreadsheet_file.name)
        project_metadata = MetadataResource.from_dict(project)
        file_metadata = exporter.create_supplementary_file_metadata(spreadsheet_file, project_metadata)
        check_file_metadata(project_metadata=project_metadata, file_metadata=file_metadata, terra_client=terra_client)


def check_spreadsheet_copied_to_terra(actual_file_metadata, project, terra_client):
    terra_client.write_to_staging_bucket.assert_called_with(
        object_key=f'{project["uuid"]["uuid"]}/data/{actual_file_metadata.full_resource["fileName"]}',
        data_stream=ANY
    )


def check_generated_links(actual_file_metadata: MetadataResource,
                          project_metadata: MetadataResource | dict,
                          terra_client):
    if isinstance(project_metadata, dict):
        project_metadata = MetadataResource.from_dict(project_metadata)
    terra_client.write_links.assert_called_with(ANY,
                                                actual_file_metadata.uuid,
                                                project_metadata.dcp_version,
                                                project_metadata.uuid)


def check_file_metadata(project_metadata: MetadataResource | dict, terra_client=None, file_metadata=None):
    if isinstance(project_metadata, dict):
        project_metadata = MetadataResource.from_dict(project_metadata)
    if terra_client and not file_metadata:
        terra_client.write_metadata.assert_called_with(ANY, project_metadata.uuid)
        if file_metadata:
            raise ValueError('Bad input. Use only one of terra_client or file_metadata arguments')
        file_metadata = terra_client.write_metadata.call_args.args[0]
    assert_that(file_metadata.metadata_json['file_core']) \
        .has_format('xlsx') \
        .has_file_source("DCP/2 Ingest") \
        .has_content_description([
        {
            "text": "metadata spreadsheet",
            "ontology": "data:2193",
            "ontology_label": "Database entry metadata"
        }
    ])
    TerraStorageClient.validate_json_doc(file_metadata.get_content())
    TerraStorageClient.update_schema_info_and_validate(FileDescriptor.from_file_metadata(file_metadata).to_dict(),
                                                       SchemaResource(schema_url='https://schema.humancellatlas.org/system/2.0.0/file_descriptor',
                                                                      schema_version='2.0.0'))
    return file_metadata
