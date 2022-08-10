import logging

from hca_ingest.api.ingestapi import IngestApi

from exporter import utils
from exporter.graph.graph_crawler import GraphCrawler
from exporter.metadata import MetadataResource, MetadataService
from exporter.terra.dcp_staging_client import DcpStagingClient
from exporter.terra.terra_export_job import TerraExportJobService

LOGGER_NAME = __name__


class TerraExporter:
    def __init__(self,
                 ingest_client: IngestApi,
                 metadata_service: MetadataService,
                 graph_crawler: GraphCrawler,
                 dcp_staging_client: DcpStagingClient,
                 job_service: TerraExportJobService):
        self.ingest_client = ingest_client
        self.metadata_service = metadata_service
        self.graph_crawler = graph_crawler
        self.dcp_staging_client = dcp_staging_client
        self.job_service = job_service

        self.logger = logging.getLogger(LOGGER_NAME)

    @utils.exec_time(logging.getLogger(LOGGER_NAME), logging.INFO)
    def export(self, process_uuid, submission_uuid, export_job_id):
        self.logger.info(f"export started")
        process = self.get_process(process_uuid)
        project = self.project_for_process(process)
        submission = self.get_submission(submission_uuid)
        export_data = "Export metadata" not in submission.get("submitActions", [])

        self.logger.info(f"The export data flag has been set to {export_data}")
        if export_data and not self.job_service.is_data_transfer_complete(export_job_id):
            self.logger.info("Exporting data files..")
            transfer_job_spec, success = self.dcp_staging_client.transfer_data_files(submission, project.uuid,
                                                                                     export_job_id)
            self._wait_for_data_transfer_to_complete(export_job_id, success, transfer_job_spec)

        self.logger.info("Exporting metadata..")
        experiment_graph = self.graph_crawler.generate_complete_experiment_graph(process, project)

        self.dcp_staging_client.write_metadatas(experiment_graph.nodes.get_nodes(), project.uuid)
        self.dcp_staging_client.write_links(experiment_graph.links, process_uuid, process.dcp_version, project.uuid)
        self.dcp_staging_client.write_staging_area_json(project.uuid)

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
            self.dcp_staging_client.wait_for_transfer_to_complete(transfer_job_spec.name, compute_wait_time,
                                                                  start_wait_time_sec, max_wait_time_sec)
            self.job_service.set_data_transfer_complete(export_job_id)
        else:
            self.logger.info("Google Cloud Transfer job was already created..")
            self.logger.info("Waiting for job to complete..")
            self.job_service.wait_for_data_transfer_to_complete(export_job_id, compute_wait_time, start_wait_time_sec,
                                                                max_wait_time_sec)

    def get_process(self, process_uuid) -> MetadataResource:
        return MetadataResource.from_dict(self.ingest_client.get_entity_by_uuid('processes', process_uuid))

    def get_submission(self, submission_uuid):
        return self.ingest_client.get_entity_by_uuid('submissionEnvelopes', submission_uuid)

    def project_for_process(self, process: MetadataResource) -> MetadataResource:
        return MetadataResource.from_dict(
            list(self.ingest_client.get_related_entities("projects", process.full_resource, "projects"))[0])
