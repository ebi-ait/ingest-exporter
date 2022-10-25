from kombu import Message
from openpyxl.workbook import Workbook

from exporter.graph.crawler import SupplementaryFilesInfo
from exporter.graph.experiment import ExperimentGraph
from exporter.ingest.service import IngestService
from exporter.ingest.spreadsheet.message import SpreadsheetGeneratorMessage
from exporter.metadata.resource import MetadataResource
from exporter.queue.handler import MessageHandler
from exporter.session_context import SessionContext
from exporter.terra.experiment.client import TerraStorageClient


class SpreadsheetHandler(MessageHandler):
    def __init__(self, ingest_service: IngestService,
                 terra_client:  TerraStorageClient):
        super().__init__('IngestSpreadsheetGenerator')
        self.ingest = ingest_service
        self.terra_client = terra_client

    def set_context(self, body: dict) -> SessionContext:
        return SessionContext(
            logger=self.logger,
            context={
                'submission_uuid': body.get('submissionUuid'),
                'export_job_id': body.get('exportJobId'),
                'project_uuid': body.get('projectUuid')
            }
        )

    def handle_message(self, body: dict, msg: Message):
        message = SpreadsheetGeneratorMessage(body)
        self.logger.info('Received spreadsheet generation message')
        gen_job_id = self.ingest.start_spreadsheet_generation(message.submission_uuid)
        self.logger.info('Started spreadsheet generation, informing ingest')
        self.ingest.set_spreadsheet_generation_id(message.job_id, gen_job_id)
        self.logger.info('Acknowledging data transfer message')
        msg.ack()

    def process_spreadsheet_metadata(self, project_uuid: str):
        # file metadata json & file descriptor
        # use Alexie's function
        metadata: MetadataResource =
        spreadsheet_file_uuid = metadata.uuid
        self.terra_client.write_metadata(metadata, project_uuid)
        # links json
        project = self.ingest.get_metadata(entity_type='project', uuid=project_uuid)
        supplementary_files_info = SupplementaryFilesInfo(for_entity=project, files=[metadata])
        experiment_graph = ExperimentGraph.from_supplementary_files_info(supplementary_files_info, project)
        self.terra_client.write_links(experiment_graph.links,
                                      spreadsheet_file_uuid,
                                      project.dcp_version,
                                      project_uuid)
        # copy spreadsheet to terra
        workbook: Workbook = None
        # object key should be proj-uuid/data/sprea
        object_key =
        self.terra_client.write_to_staging_bucket(object_key=,
                                                  data_stream=)
        pass