import logging

from exporter.ingest.service import IngestService
from exporter.terra.submission.client import TerraTransferClient


class TerraSubmissionExporter:
    def __init__(self, ingest_service: IngestService, terra_client: TerraTransferClient):
        self.ingest_service = ingest_service
        self.terra_client = terra_client
        self.logger = logging.getLogger('TerraSubmissionExporter')

    def start_data_file_transfer(self, job_id: str, submission_uuid: str, project_uuid: str):
        self.logger.info(f"data sync starting")
        submission = self.ingest_service.get_submission(submission_uuid)
        export_data = "Export metadata" not in submission.get("submitActions", [])

        self.logger.info(f"The export data flag has been set to {export_data}")
        if export_data and not self.ingest_service.is_data_transfer_complete(job_id):
            self.logger.info("Exporting data files..")
            transfer_job_spec, success = self.terra_client.transfer_data_files(
                submission,
                project_uuid,
                job_id
            )
            self._wait_for_data_transfer_to_complete(job_id, success, transfer_job_spec)

    # Only the exporter process which is successful should be polling GCP Transfer service if the job is complete
    # This is to avoid hitting the rate limit 500 requests per 100 sec https://cloud.google.com/storage-transfer/quotas
    def _wait_for_data_transfer_to_complete(self, export_job_id, success, transfer_job_spec):
        def compute_wait_time(start_wait_time_sec):
            max_wait_interval_sec = 10 * 60
            return min(start_wait_time_sec * 2, max_wait_interval_sec)

        max_wait_time_sec = 60 * 60 * 6
        start_wait_time_sec = 2
        if success:
            self.logger.info("Google Cloud Transfer job was successfully created..")
            self.logger.info("Waiting for job to complete..")
            self.terra_client.wait_for_transfer_to_complete(transfer_job_spec.name, compute_wait_time,
                                                                  start_wait_time_sec, max_wait_time_sec)
            self.ingest_service.set_data_transfer_complete(export_job_id)
        else:
            self.logger.info("Google Cloud Transfer job was already created..")
            self.logger.info("Waiting for job to complete..")
            self.ingest_service.wait_for_data_transfer_to_complete(export_job_id, compute_wait_time, start_wait_time_sec,
                                                                   max_wait_time_sec)
