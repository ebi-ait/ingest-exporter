import hashlib
import logging
import uuid
from tempfile import NamedTemporaryFile as TempFile

import crc32c
from hca_ingest.downloader.workbook import WorkbookDownloader

from exporter.graph.info.supplementary_files import SupplementaryFilesInfo
from exporter.graph.experiment import ExperimentGraph
from exporter.ingest.service import IngestService
from exporter.metadata.resource import MetadataResource as Metadata
from exporter.terra.storage import TerraStorageClient


class SpreadsheetExporter:
    def __init__(self, ingest_service: IngestService, terra_client: TerraStorageClient):
        self.ingest = ingest_service
        self.terra = terra_client
        self.downloader = WorkbookDownloader(self.ingest.api)
        self.logger = logging.getLogger('IngestSpreadsheetExporter')

    def export_spreadsheet(self, job_id: str, project_uuid: str, submission_uuid: str):
        self.logger.info("Generating Spreadsheet")
        workbook = self.downloader.get_workbook_from_submission(submission_uuid)
        self.logger.info("Generating Spreadsheet Metadata")
        project_meta = self.ingest.get_metadata(entity_type='projects', uuid=project_uuid)
        with TempFile() as spreadsheet_file:
            workbook.save(spreadsheet_file.name)
            # todo: make it available in broker as well.
            file_meta = self.create_supplementary_file_metadata(spreadsheet_file, project_meta)
            self.logger.info("Writing to Terra")
            self.write_to_terra(spreadsheet_file, project_meta, file_meta)

    def write_to_terra(self, spreadsheet_file: TempFile, project_meta: Metadata, file_meta: Metadata):
        self.terra.write_metadata(file_meta, project_meta.uuid)
        self.write_links(file_meta, project_meta)
        spreadsheet_file.seek(0)
        self.terra.write_to_staging_bucket(
            object_key=f'{project_meta.uuid}/data/{file_meta.full_resource["fileName"]}',
            data_stream=spreadsheet_file
        )

    def write_links(self, file_meta: Metadata, project_meta: Metadata):
        info = SupplementaryFilesInfo(for_entity=project_meta, files=[file_meta])
        experiment_graph = ExperimentGraph.from_supplementary_files_info(info, project_meta)
        self.terra.write_links(
            experiment_graph.links,
            file_meta.uuid,
            file_meta.dcp_version,
            project_meta.uuid
        )

    def create_supplementary_file_metadata(self, spreadsheet_file: TempFile, project_meta: Metadata) -> Metadata:
        schema_url = self.ingest.api.get_latest_schema_url('type', 'file', 'supplementary_file')
        filename = f'metadata_{project_meta.uuid}.xlsx'
        spreadsheet_file.seek(0)
        spreadsheet_bytes = spreadsheet_file.read()
        spreadsheet_size = len(spreadsheet_bytes)
        s256 = hashlib.sha256(spreadsheet_bytes)
        s1 = hashlib.sha1(spreadsheet_bytes)
        crc = f'{crc32c.crc32c(spreadsheet_bytes):08x}'
        metadata_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f'{filename}_metadata'))
        datafile_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f'{filename}_data'))
        return Metadata.from_dict({
            "fileName": filename,
            "dataFileUuid": datafile_uuid,
            "cloudUrl": None,
            "fileContentType": "xlsx",
            "size": spreadsheet_size,
            "checksums": {
                "sha256": s256.hexdigest(),
                "crc32c": crc,
                "sha1": s1.hexdigest(),
                "s3_etag": 'n/a - not in s3'
            },
            "uuid": {"uuid": metadata_uuid},
            "dcpVersion": project_meta.dcp_version,
            "type": "file",
            "submissionDate": project_meta.full_resource['submissionDate'],
            "updateDate": project_meta.full_resource['updateDate'],
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
