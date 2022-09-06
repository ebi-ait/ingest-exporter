from exporter.ingest.service import IngestService
from exporter.session_context import SessionContext
from exporter.terra.exceptions import SubmissionDoesNotHaveRequiredAction
from exporter.terra.submission.client import TerraTransferClient


class TerraSubmissionExporter:
    def __init__(self, ingest_service: IngestService, terra_client: TerraTransferClient):
        self.ingest_service = ingest_service
        self.terra_client = terra_client
        self.logger = SessionContext.register_logger(__name__)

    def start_data_file_transfer(self, job_id: str, submission_uuid: str, project_uuid: str):
        self.logger.info(f"Getting Submission for data transfer")
        submission = self.ingest_service.get_submission(submission_uuid)
        if "Export" not in submission.get("submitActions", []):
            self.logger.error("The export data flag has not been set!")
            raise SubmissionDoesNotHaveRequiredAction()
        self.logger.info(f"Starting data transfer")
        self.terra_client.transfer_data_files(submission, project_uuid, job_id)
