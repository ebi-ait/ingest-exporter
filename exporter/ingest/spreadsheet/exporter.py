# from hca_ingest.downloader.workbook import WorkbookDownloader
from openpyxl.workbook import Workbook

from exporter.graph.crawler import SupplementaryFilesInfo
from exporter.graph.experiment import ExperimentGraph
from exporter.ingest.service import IngestService
from exporter.metadata.resource import MetadataResource
from exporter.terra.experiment.client import TerraStorageClient


class SpreadsheetExporter:
    def __init__(self, ingest_service: IngestService, terra_client:  TerraStorageClient):
        self.ingest = ingest_service
        self.terra = terra_client
        # self.downloader = WorkbookDownloader(ingest_service.ingest_client)

    def export_spreadsheet(self, job_id: str, project_uuid: str, submission_uuid: str):
        # Inform ingest that the generation has started
        # download workbook
        #   self.downloader.get_workbook_from_submission(msg.submission_uuid)
        # get metadata
        #   self.process_spreadsheet_metadata
        # send files to terra
        # Inform ingest that the generation has finished
        pass

    def process_spreadsheet_metadata(self, project_uuid: str):
        # file metadata json & file descriptor
        # use Alexie's function
        metadata: MetadataResource = {}
        spreadsheet_file_uuid = metadata.uuid
        self.terra.write_metadata(metadata, project_uuid)
        # links json
        project = self.ingest.get_metadata(entity_type='project', uuid=project_uuid)
        supplementary_files_info = SupplementaryFilesInfo(for_entity=project, files=[metadata])
        experiment_graph = ExperimentGraph.from_supplementary_files_info(supplementary_files_info, project)
        self.terra.write_links(experiment_graph.links,
                                      spreadsheet_file_uuid,
                                      project.dcp_version,
                                      project_uuid)
        # copy spreadsheet to terra
        workbook: Workbook = None
        # object key should be proj-uuid/data/sprea
        object_key = 'proj-uuid/data/sprea'
        self.terra.write_to_staging_bucket(object_key='', data_stream='')
        pass
