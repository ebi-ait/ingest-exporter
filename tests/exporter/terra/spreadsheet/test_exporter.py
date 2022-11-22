import uuid
from datetime import datetime
from tempfile import NamedTemporaryFile
from unittest.mock import Mock, ANY

import pytest
from assertpy import assert_that
from hca_ingest.api.ingestapi import IngestApi
from hca_ingest.utils.date import date_to_json_string, parse_date_string
from openpyxl.workbook import Workbook

from exporter.ingest.service import IngestService
from exporter.metadata.descriptor import FileDescriptor
from exporter.metadata.resource import MetadataResource
from exporter.schema.resource import SchemaResource
from exporter.terra.spreadsheet.exporter import SpreadsheetExporter
from exporter.terra.storage import TerraStorageClient


@pytest.fixture
def ingest_api(mocker):
    ingest_api: IngestApi = mocker.Mock(spec=IngestApi)
    ingest_api.get_latest_schema_url.return_value = 'https://schema.humancellatlas.org/type/file/2.5.0/supplementary_file'
    return ingest_api


@pytest.fixture
def ingest_service(mocker, ingest_api, project, spreadsheet_dcp_version):
    ingest_service: IngestService = mocker.Mock(spec=IngestService)
    ingest_service.api = ingest_api
    ingest_service.get_metadata.return_value = project
    ingest_service.get_submission_dcp_version_from_uuid.return_value = spreadsheet_dcp_version
    return ingest_service


@pytest.fixture
def service_with_new_spreadsheet(ingest_service, new_spreadsheet_version):
    ingest_service.get_submission_dcp_version_from_uuid.return_value = new_spreadsheet_version
    return ingest_service


@pytest.fixture
def spreadsheet_dcp_version() -> str:
    return "2022-06-13T14:32:59.593000Z"


@pytest.fixture
def new_spreadsheet_version() -> str:
    return date_to_json_string(datetime.now())


@pytest.fixture
def submission_uuid():
    return str(uuid.uuid4())


@pytest.fixture
def new_submission_uuid():
    return str(uuid.uuid4())


@pytest.fixture
def project_uuid():
    return str(uuid.uuid4())


@pytest.fixture(params=[{}, {"project_short_name": "Test_Project"}], ids=["project without short name", "project with short name"])
def project_dict(project_uuid, request):
    return {
        "uuid": {"uuid": project_uuid},
        "dcpVersion": "2022-05-29T13:51:08.593000Z",
        "content": {
            "describedBy": "https://schema.humancellatlas.org/type/project/17.0.0/project",
            "project_core": request.param
        },
        "type": "project",
        "submissionDate": "2022-03-28T13:51:08.593000Z",
        "updateDate": "2022-05-28T13:51:08.593000Z",
    }


@pytest.fixture
def project(project_dict) -> MetadataResource:
    return MetadataResource.from_dict(project_dict)


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


@pytest.fixture
def exporter_with_new_spreadsheet(service_with_new_spreadsheet, terra_client, workbook, mocker):
    exporter = SpreadsheetExporter(service_with_new_spreadsheet, terra_client)
    exporter.downloader.get_workbook_from_submission = mocker.Mock(return_value=workbook)
    return exporter


@pytest.fixture()
def failing_exporter(ingest_service, terra_client, mocker):
    exporter = SpreadsheetExporter(ingest_service, terra_client)
    exporter.downloader.get_workbook_from_submission = mocker.Mock(
        side_effect=RuntimeError('spreadsheet generation problem')
    )
    return exporter


@pytest.fixture
def initial_supplementary_file(terra_client, exporter, project, submission_uuid):
    exporter.export_spreadsheet(project.uuid, submission_uuid)
    return check_file_metadata(project, terra_client=terra_client)


@pytest.fixture
def supplementary_file_from_new_export(terra_client, exporter_with_new_spreadsheet, project, submission_uuid):
    exporter_with_new_spreadsheet.export_spreadsheet(project.uuid, submission_uuid)
    return check_file_metadata(project, terra_client=terra_client)


@pytest.fixture
def supplementary_file_from_new_submission(terra_client, exporter_with_new_spreadsheet, project, new_submission_uuid):
    exporter_with_new_spreadsheet.export_spreadsheet(project.uuid, new_submission_uuid)
    return check_file_metadata(project, terra_client=terra_client)


def test_happy_path(exporter: SpreadsheetExporter,
                    terra_client: Mock,
                    project: MetadataResource,
                    submission_uuid: str,
                    caplog):
    # when
    exporter.export_spreadsheet(project.uuid, submission_uuid)

    # then
    actual_file_metadata = check_file_metadata(project, terra_client=terra_client)
    check_generated_links(terra_client, project, actual_file_metadata)
    check_spreadsheet_copied_to_terra(actual_file_metadata, project, terra_client)
    assert "Generating Spreadsheet" in caplog.text


def test_exception_during_export(failing_exporter: SpreadsheetExporter, project_uuid, submission_uuid, caplog):
    # given an exception is thrown while generating the spreadsheet

    # when
    with pytest.raises(RuntimeError):
        failing_exporter.export_spreadsheet(project_uuid, submission_uuid)


def test_spreadsheet_metadata_on_submission_update(initial_supplementary_file, supplementary_file_from_new_export):
    check_file_prefix_matches(initial_supplementary_file, supplementary_file_from_new_export)
    check_uuids_match(initial_supplementary_file, supplementary_file_from_new_export)
    check_dates_differ(initial_supplementary_file, supplementary_file_from_new_export)


def test_spreadsheet_metadata_on_new_submission(initial_supplementary_file, supplementary_file_from_new_submission):
    check_file_prefix_matches(initial_supplementary_file, supplementary_file_from_new_submission)
    check_uuids_differ(initial_supplementary_file, supplementary_file_from_new_submission)
    check_dates_differ(initial_supplementary_file, supplementary_file_from_new_submission)


def check_file_prefix_matches(initial, new):
    assert_that(get_file_info(initial)['project_shortname_or_uuid']).is_equal_to(get_file_info(new)['project_shortname_or_uuid'])


def check_uuids_match(initial, new):
    assert_that(initial.uuid).is_equal_to(new.uuid)
    assert_that(initial.full_resource['dataFileUuid']).is_equal_to(new.full_resource['dataFileUuid'])


def check_uuids_differ(initial, new):
    assert_that(initial.uuid).is_not_equal_to(new.uuid)
    assert_that(initial.full_resource['dataFileUuid']).is_not_equal_to(new.full_resource['dataFileUuid'])


def check_dates_differ(initial, new):
    assert_that(initial.dcp_version).is_not_equal_to(new.dcp_version)
    assert_that(get_file_info(initial)['date']).is_not_equal_to(get_file_info(new)['date'])


def check_spreadsheet_copied_to_terra(actual_file_metadata: MetadataResource,
                                      project: MetadataResource, terra_client):
    terra_client.write_to_staging_bucket.assert_called_with(
        object_key=f'{project.uuid}/data/{actual_file_metadata.full_resource["fileName"]}',
        data_stream=ANY,
        overwrite=True
    )


def check_generated_links(terra_client, project_metadata: MetadataResource, file_metadata: MetadataResource):
    terra_client.write_links.assert_called_with(
        ANY,
        file_metadata.uuid,
        file_metadata.dcp_version,
        project_metadata.uuid,
    )


def check_file_metadata(project_metadata: MetadataResource, file_metadata=None, terra_client=None) -> MetadataResource:
    if terra_client and not file_metadata:
        terra_client.write_metadata.assert_called_with(ANY, project_metadata.uuid)
        if file_metadata:
            raise ValueError('Bad input. Use only one of terra_client or file_metadata arguments')
        file_metadata = terra_client.write_metadata.call_args.args[0]
    assert_that(file_metadata.metadata_json['file_core']) \
        .has_format('xlsx') \
        .has_file_source("DCP/2 Ingest") \
        .has_content_description([{
            "text": "metadata spreadsheet",
            "ontology": "data:2193",
            "ontology_label": "Database entry metadata"
        }])
    filename = file_metadata.full_resource['fileName']
    short_name = project_metadata.metadata_json.get('project_core', {}).get('project_short_name')
    assert_that(filename) \
        .starts_with(short_name if short_name else project_metadata.uuid) \
        .contains('_metadata_') \
        .contains(parse_date_string(file_metadata.dcp_version).strftime('%d-%m-%Y')) \
        .ends_with('.xlsx')
    TerraStorageClient.validate_json_doc(file_metadata.get_content())
    TerraStorageClient.update_schema_info_and_validate(
        FileDescriptor.from_file_metadata(file_metadata).to_dict(),
        SchemaResource(schema_url='https://schema.humancellatlas.org/system/2.0.0/file_descriptor', schema_version='2.0.0'))
    return file_metadata


def get_file_info(file: MetadataResource):
    name_split = file.full_resource['fileName'].split('_metadata_')
    return {
        'project_shortname_or_uuid': name_split[0],
        'date': name_split[1]
    }
