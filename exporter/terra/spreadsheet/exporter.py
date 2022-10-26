import hashlib
import logging
import os
import uuid
from tempfile import NamedTemporaryFile

import crc32c as crc32c
from hca_ingest.downloader.workbook import WorkbookDownloader

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
        self.logger.info("Generating Spreadsheet")
        workbook = self.downloader.get_workbook_from_submission(submission_uuid)
        self.logger.info("Generating Metadata")
        project = self.ingest.get_metadata(entity_type='project', uuid=project_uuid)
        with NamedTemporaryFile() as spreadsheet:
            workbook.save(spreadsheet.name)
            file = self.create_supplementary_file_metadata(spreadsheet, project)
            self.logger.info("Writing to Terra")
            self.write_to_terra(spreadsheet, project, file)

    def write_to_terra(self, spreadsheet: NamedTemporaryFile, project: MetadataResource, file: MetadataResource):
        self.terra.write_metadata(file, project.uuid)
        self.write_links(file, project)
        spreadsheet.seek(0)
        spreadsheet_bytes = spreadsheet.read()
        self.terra.write_to_staging_bucket(
            object_key=f'{project.uuid}/data/{file.full_resource["fileName"]}',
            data_stream=spreadsheet_bytes
        )

    def write_links(self, file: MetadataResource, project: MetadataResource):
        supplementary_files_info = SupplementaryFilesInfo(for_entity=project, files=[file])
        experiment_graph = ExperimentGraph.from_supplementary_files_info(supplementary_files_info, project)
        self.terra.write_links(
            experiment_graph.links,
            file.uuid,
            file.dcp_version,
            project.uuid
        )

    def create_supplementary_file_metadata(self, spreadsheet: NamedTemporaryFile, project: MetadataResource) -> MetadataResource:
        schema_url = self.ingest.api.get_latest_schema_url('type', 'file', 'supplementary_file')
        filename = f'metadata_{project.uuid}.xlsx'
        # ToDo: This can be way better but I'm out of time!
        spreadsheet.seek(0, os.SEEK_END)
        size_in_bytes = spreadsheet.tell()
        spreadsheet.seek(0)
        spreadsheet_bytes = spreadsheet.read()
        s256 = hashlib.sha256(spreadsheet_bytes)
        s1 = hashlib.sha1(spreadsheet_bytes)
        crc = crc32c.crc32c(spreadsheet_bytes)
        return MetadataResource.from_dict({
            "fileName": filename,
            "dataFileUuid": str(uuid.uuid4()),
            "cloudUrl": None,
            "fileContentType": "xlsx",
            "size": size_in_bytes,
            "checksums": {
                "sha256": s256.hexdigest(),
                "crc32c": crc,
                "sha1": s1.hexdigest(),
                "s3_etag": None
            },
            "uuid": {"uuid": str(uuid.uuid4())},
            "dcpVersion": project.dcp_version,
            "type": "file",
            "submissionDate": "",  # ToDo: get from submission?
            "updateDate": "",  # ToDo: get from submission?
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
