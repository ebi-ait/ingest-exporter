import json
import random
import uuid
from datetime import datetime

import pytest
from assertpy import assert_that
from hca_ingest.utils.date import date_to_json_string, parse_date_string

from exporter.ingest.export_job import ExportJob, ExportJobState, ExportContextState

CONTEXT_STATUSES = ['', 'NOT_STARTED', 'STARTED', 'COMPLETE']


@pytest.fixture
def created_date() -> str:
    return "2022-11-24T10:31:33.265Z"


@pytest.fixture
def now() -> datetime:
    return datetime.now()


@pytest.fixture
def updated_date(now: datetime) -> str:
    return date_to_json_string(now)


@pytest.fixture(params=[True, False], ids=['with project uuid', 'missing project uuid'])
def project_uuid(request):
    if request.param:
        return str(uuid.uuid4())
    return ''


@pytest.fixture(params=['Exporting', 'Failed', 'Exported', 'Deprecated'])
def status_str(request) -> str:
    return request.param


@pytest.fixture(params=CONTEXT_STATUSES)
def data_file_str(request) -> str:
    return request.param


@pytest.fixture(params=CONTEXT_STATUSES)
def spreadsheet_str(request) -> str:
    return request.param


@pytest.fixture
def status(status_str: str) -> ExportJobState:
    return ExportJobState(status_str.upper())


@pytest.fixture
def data_file_status(data_file_str: str) -> ExportContextState:
    if data_file_str:
        return ExportContextState(data_file_str)
    return ExportContextState.NOT_STARTED


@pytest.fixture
def spreadsheet_status(spreadsheet_str: str) -> ExportContextState:
    if spreadsheet_str:
        return ExportContextState(spreadsheet_str)
    return ExportContextState.NOT_STARTED


@pytest.fixture(params=[True, False], ids=['with count', 'missing count'])
def assay_count(request) -> int:
    if request.param:
        return random.randint(1, 99999)
    return 0


@pytest.fixture(params=[True, False], ids=['with links', 'missing links'])
def job_id(request) -> str:
    if request.param:
        return str(uuid.uuid4()).replace('-', '')
    return ''


@pytest.fixture(params=[True, False], ids=['with submission', 'missing submission'])
def submission_id(request) -> str:
    if request.param:
        return str(uuid.uuid4()).replace('-', '')
    return ''


@pytest.fixture
def submission_url(api_url, submission_id):
    url = f'{api_url}/submissionEnvelopes'
    if submission_id:
        url += f'/{submission_id}'
    return url


@pytest.fixture
def api_url() -> str:
    return 'https://api.ingest.dev.archive.data.humancellatlas.org'


@pytest.fixture
def job(job_dict: dict) -> ExportJob:
    return ExportJob(job_dict)


def test_all_params(job, job_id, created_date, now, status, data_file_status, spreadsheet_status, assay_count, submission_id):
    assert_that(job.job_id).is_equal_to(job_id)
    assert_that(job.submission_id).is_equal_to(submission_id)
    assert_that(job.created_date).is_equal_to(parse_date_string(created_date))
    assert_that(job.updated_date).is_equal_to(now)
    assert_that(job.export_state).is_equal_to(status)
    assert_that(job.data_file_transfer).is_equal_to(data_file_status)
    assert_that(job.spreadsheet_generation).is_equal_to(spreadsheet_status)
    assert_that(job.num_expected_assays).is_equal_to(assay_count)


def test_empty_job():
    job = ExportJob({})
    assert_that(job.job_id).is_equal_to('')
    assert_that(job.submission_id).is_equal_to('')
    assert_that(job.created_date).is_equal_to(datetime.min)
    assert_that(job.updated_date).is_equal_to_ignoring_seconds(datetime.now())
    assert_that(job.export_state).is_equal_to(ExportJobState.EXPORTING)
    assert_that(job.data_file_transfer).is_equal_to(ExportContextState.NOT_STARTED)
    assert_that(job.spreadsheet_generation).is_equal_to(ExportContextState.NOT_STARTED)
    assert_that(job.num_expected_assays).is_equal_to(0)


def test_broken_context_job(broken_context_job_dict):
    job = ExportJob(broken_context_job_dict)
    assert_that(job.job_id).is_equal_to('638e37f931a4c47b19a7cc57')
    assert_that(job.submission_id).is_equal_to('638a7f9e31a4c47b19a7c18e')
    assert_that(job.export_state).is_equal_to(ExportJobState.EXPORTING)
    assert_that(job.data_file_transfer).is_equal_to(ExportContextState.COMPLETE)
    assert_that(job.spreadsheet_generation).is_equal_to(ExportContextState.NOT_STARTED)
    assert_that(job.num_expected_assays).is_equal_to(6)


@pytest.fixture
def broken_context_job_dict():
    with open('tests/exporter/ingest/broken_export_job.json', 'r', encoding='utf-8') as f:
        return json.load(f)


@pytest.fixture
def job_dict(created_date, project_uuid, status_str, updated_date, data_file_str, spreadsheet_str, assay_count, api_url, job_id, submission_url):
    base = {
        "createdDate": created_date,
        "destination": {
            "name": "Dcp",
            "version": "v2",
            "context": {}
        },
        "status": status_str,
        "updatedDate": updated_date,
        "context": {},
        "errors": []
    }
    if project_uuid:
        base['destination']['context']['projectUuid'] = project_uuid
    if data_file_str:
        base['context']['dataFileTransfer'] = data_file_str
    if spreadsheet_str:
        base['context']['spreadsheetGeneration'] = spreadsheet_str
    if assay_count:
        base['context']['totalAssayCount'] = assay_count
    if job_id:
        base['_links'] = {
            "self": {
                "href": f'{api_url}/exportJobs/{job_id}'
            },
            "exportJob": {
                "href": f'{api_url}/exportJobs/{job_id}'
            },
            "exportEntities": {
                "href": f'{api_url}/exportJobs/{job_id}/entities'
            },
            "exportEntitiesByStatus": {
                "href": f'{api_url}/exportJobs/{job_id}/entities' + "?status={status}",
                "templated": True
            }
        }
    if submission_url:
        base.setdefault('_links', {})['submission'] = {
            "href": submission_url
        }
    return base
