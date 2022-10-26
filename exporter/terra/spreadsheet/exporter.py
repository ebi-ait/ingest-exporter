import logging
import uuid
from tempfile import NamedTemporaryFile

from hca_ingest.downloader.workbook import WorkbookDownloader
from openpyxl.workbook import Workbook

from exporter.graph.info.supplementary_files import SupplementaryFilesInfo
from exporter.graph.experiment import ExperimentGraph
from exporter.ingest.service import IngestService
from exporter.metadata.resource import MetadataResource
from exporter.terra.storage import TerraStorageClient


class SpreadsheetExporter:
    def __init__(self, ingest_service: IngestService, terra_client:  TerraStorageClient):
        self.ingest = ingest_service
        self.terra = terra_client
        self.downloader = WorkbookDownloader(self.ingest.api)
        self.logger = logging.getLogger('IngestSpreadsheetExporter')

    def export_spreadsheet(self, job_id: str, project_uuid: str, submission_uuid: str):
        # Inform ingest that the generation has started
        submission = self.ingest.get_submission(submission_uuid)
        submission_url = submission['_links']['self']['href']
        staging_area = submission['stagingDetails']['stagingAreaLocation']['value']
        # download workbook
        workbook = self.downloader.get_workbook_from_submission(submission_uuid)
        # self.save_spreadsheet(spreadsheet_details, workbook)
        self.process_spreadsheet_metadata(project_uuid, workbook)
        self.logger.info(f'Done exporting spreadsheet for submission {submission_uuid}!')
        # get metadata
        # Inform ingest that the generation has finished

    def process_spreadsheet_metadata(self, project_uuid: str, workbook: Workbook):
        schema_url = self.ingest.api.get_latest_schema_url('type', 'file', 'supplementary_file')
        project = self.ingest.get_metadata(entity_type='project', uuid=project_uuid)
        filename = f'metadata_{project_uuid}.xlsx'
        file_metadata: MetadataResource = self.build_supplementary_file_payload(schema_url, filename, project)
        spreadsheet_file_uuid = file_metadata.uuid
        self.terra.write_metadata(file_metadata, project_uuid)
        # links json
        self.write_links(file_metadata, project)
        # copy spreadsheet to terra
        with NamedTemporaryFile() as tmp:
            workbook.save(tmp.name)
            tmp.seek(0)
            stream = tmp.read()
            self.terra.write_to_staging_bucket(object_key=f'{project_uuid}/data/{filename}', data_stream=stream)
        pass

    def write_links(self, file_metadata:MetadataResource, project:MetadataResource):
        supplementary_files_info = SupplementaryFilesInfo(for_entity=project, files=[file_metadata])
        experiment_graph = ExperimentGraph.from_supplementary_files_info(supplementary_files_info, project)
        self.terra.write_links(experiment_graph.links,
                               file_metadata.uuid,
                               file_metadata.dcp_version,
                               project.uuid)

    @staticmethod
    def build_supplementary_file_payload(schema_url: str, filename: str, project) -> MetadataResource:
        return MetadataResource.from_dict({
            "fileName": filename,
            "dataFileUuid": str(uuid.uuid4()),
            "cloudUrl": None,
            "fileContentType": "xlsx",
            "size": None,
            "checksums": {
                "sha256": "",
                "crc32c": "",
                "sha1": "",
                "s3_etag": ""
            },
            "uuid": {"uuid": str(uuid.uuid4())},
            "dcpVersion": project.dcpVersion,
            "type": "file",
            "submissionDate": "",
            "updateDate": "",
            "content": {
                "describedBy": schema_url,
                "schema_type": "file",
                "file_core": {
                    "file_name": filename,
                    "format": "xlsx",
                    "file_source": "DCP/2 Ingest",
                    "content_description": [
                        {
                            "text": "metadata spreadsheet",
                            "ontology": "data:2193",
                            "ontology_label": "Database entry metadata"
                        }
                    ]
                }
            }
        })
