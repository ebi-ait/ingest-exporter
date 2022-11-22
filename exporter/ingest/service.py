from hca_ingest.api.ingestapi import IngestApi

from exporter.ingest.export_job import ExportEntity, ExportJobState, ExportJob, ExportContextState
from exporter.metadata.resource import MetadataResource
from exporter.session_context import SessionContext


class IngestService:
    def __init__(self, ingest_client: IngestApi):
        self.api = ingest_client
        self.logger = SessionContext.register_logger(__name__)

    def create_export_entity(self, job_id: str, assay_process_id: str):
        assay_export_entity = ExportEntity(assay_process_id, [])
        create_export_entity_url = self.get_export_entities_url(job_id)
        self.api.post(
            create_export_entity_url,
            json=assay_export_entity.to_dict()
        )
        self._maybe_complete_job(job_id)

    def _maybe_complete_job(self, job_id):
        export_job = self.get_job(job_id)
        self.logger.info(f'export_job.num_expected_assays: {export_job.num_expected_assays}')
        complete_entities_for_job = self.get_num_complete_entities_for_job(job_id)
        self.logger.info(f'complete_entities_for_job: {complete_entities_for_job}')
        if export_job.num_expected_assays == complete_entities_for_job:
            self.complete_job(job_id)
            self.logger.info('job complete')
        else:
            self.logger.info('job not yet complete')

    def complete_job(self, job_id: str):
        job_url = self.get_job_url(job_id)
        self.api.patch(job_url, json={"status": ExportJobState.EXPORTED.value})

    def get_job(self, job_id: str) -> ExportJob:
        job_url = self.get_job_url(job_id)
        return ExportJob(self.api.get(job_url).json())

    def job_exists(self, job_id: str) -> bool:
        job_url = self.get_job_url(job_id)
        response = self.api.session.get(job_url, headers=self.api.get_headers())
        return response.ok

    def get_job_url(self, job_id: str) -> str:
        return self.api.get_full_url(f'/exportJobs/{job_id}')

    def get_export_entities_url(self, job_id: str) -> str:
        return self.api.get_full_url(f'/exportJobs/{job_id}/entities')

    def get_metadata(self, entity_type, uuid) -> MetadataResource:
        return MetadataResource.from_dict(self.api.get_entity_by_uuid(entity_type, uuid))

    def get_submission(self, submission_uuid):
        return self.api.get_submission_by_uuid(submission_uuid)

    def project_for_process(self, process: MetadataResource) -> MetadataResource:
        return MetadataResource.from_dict(list(self.api.get_related_entities(
            "projects",
            process.full_resource,
            "projects"))[0])

    def get_num_complete_entities_for_job(self, job_id: str) -> int:
        entities_url = self.get_export_entities_url(job_id)
        find_entities_by_status_url = f'{entities_url}?status={ExportJobState.EXPORTED.value}'
        return int(self.api.get(find_entities_by_status_url).json()["page"]["totalElements"])

    def set_data_file_transfer(self, export_job_id: str, state: ExportContextState) -> ExportJob:
        return self.__set_export_job_context_state(export_job_id, "dataFileTransfer", state)

    def set_spreadsheet_generation(self, export_job_id: str, state: ExportContextState) -> ExportJob:
        return self.__set_export_job_context_state(export_job_id, "spreadsheetGeneration", state)

    def __set_export_job_context_state(self, job_id: str, context: str, state: ExportContextState) -> ExportJob:
        job_url = self.get_job_url(job_id)
        return self.api.patch(f'{job_url}/context', json={context: state.value}).json()
