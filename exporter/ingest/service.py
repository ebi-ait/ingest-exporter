import json
import logging
from typing import Callable

import polling
import requests
from hca_ingest.api.ingestapi import IngestApi

from exporter.ingest.export_job import ExportEntity, ExportJobState, ExportJob


class IngestService:
    def __init__(self, ingest_client: IngestApi):
        self.ingest_client = ingest_client
        self.logger = logging.getLogger(__name__)

    def create_export_entity(self, job_id: str, assay_process_id: str):
        assay_export_entity = ExportEntity(assay_process_id, [])
        create_export_entity_url = self.get_export_entities_url(job_id)
        requests.post(create_export_entity_url, json.dumps(assay_export_entity.to_dict()),
                      headers={"Content-type": "application/json"}, json=True).raise_for_status()
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
        self.ingest_client.patch(job_url, {"status": ExportJobState.EXPORTED.value})

    def get_job_state(self, job_id: str) -> ExportJobState:
        return self.get_job(job_id).export_state

    def get_job(self, job_id: str) -> ExportJob:
        job_url = self.get_job_url(job_id)
        return ExportJob.from_dict(self.ingest_client.get(job_url).json())

    def get_job_url(self, job_id: str) -> str:
        return self.ingest_client.get_full_url(f'/exportJobs/{job_id}')

    def get_export_entities_url(self, job_id: str) -> str:
        return self.ingest_client.get_full_url(f'/exportJobs/{job_id}/entities')

    def get_num_complete_entities_for_job(self, job_id: str) -> int:
        entities_url = self.get_export_entities_url(job_id)
        find_entities_by_status_url = f'{entities_url}?status={ExportJobState.EXPORTED.value}'
        return int(self.ingest_client.get(find_entities_by_status_url).json()["page"]["totalElements"])

    def set_data_transfer_complete(self, job_id: str):
        job_url = self.get_job_url(job_id)
        job = self.ingest_client.get(job_url).json()
        context = job["context"]
        context.update({"isDataTransferComplete": True})
        self.ingest_client.patch(job_url, {"context": context})

    def is_data_transfer_complete(self, job_id: str):
        return self.get_job(job_id).is_data_transfer_complete

    def wait_for_data_transfer_to_complete(self, job_id: str, compute_wait_time: Callable, start_wait_time_sec, max_wait_time_sec: int):
        try:
            polling.poll(
                lambda: self.is_data_transfer_complete(job_id),
                step=start_wait_time_sec,
                step_function=compute_wait_time,
                timeout=max_wait_time_sec
            )
        except polling.TimeoutException as te:
            raise
