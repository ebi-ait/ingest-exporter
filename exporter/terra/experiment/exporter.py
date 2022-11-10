import logging

from exporter.graph.crawler import GraphCrawler
from exporter.ingest.service import IngestService
from exporter.terra.storage import TerraStorageClient


class TerraExperimentExporter:
    def __init__(
            self,
            ingest_service: IngestService,
            graph_crawler: GraphCrawler,
            terra_client: TerraStorageClient,
            logger_name: str = __name__
    ):
        self.graph_crawler = graph_crawler
        self.terra_client = terra_client
        self.ingest_service = ingest_service
        self.logger = logging.getLogger(logger_name)

    def export(self, process_uuid):
        process = self.ingest_service.get_metadata('processes', process_uuid)
        project = self.ingest_service.project_for_process(process)

        self.logger.info("Exporting Experiment metadata")
        experiment_graph = self.graph_crawler.generate_complete_experiment_graph(process, project)

        self.terra_client.write_metadatas(experiment_graph.nodes.get_nodes(), project.uuid)
        self.terra_client.write_links(experiment_graph.links, process_uuid, process.dcp_version, project.uuid)
        self.terra_client.write_staging_area_json(project.uuid)
