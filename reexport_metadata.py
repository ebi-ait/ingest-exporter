import logging
import os
import sys

from threading import Thread
from multiprocessing.pool import ThreadPool

from hca_ingest.api.ingestapi import IngestApi

from exporter.ingest.service import IngestService
from exporter.schema.service import SchemaService
from exporter.session_context import SessionContext
from exporter.terra.config import TerraConfig
from exporter.terra.gcs.config import GcpConfig
from exporter.terra.gcs.storage import GcsStorage
from exporter.terra.experiment.exporter import TerraExperimentExporter
from exporter.terra.storage import TerraStorageClient
from exporter.graph.crawler import GraphCrawler
from exporter.metadata.service import MetadataService


def get_all_assay_processes_uuids(submission_uuid, api: IngestApi):
    submission = api.get_submission_by_uuid(submission_uuid)
    processes = list(api.get_entities(submission['_links']['self']['href'], 'processes', 'processes'))
    assay_processes = []
    for process in processes:
        related_files = list(api.get_related_entities('derivedFiles', process, 'files'))
        if related_files:
            assay_processes.append(process)
    return [process['uuid']['uuid'] for process in assay_processes]


def export_processes(process_uuid):
    logging.basicConfig(level=logging.CRITICAL)
    LOGGER_NAME = __name__
    logger = SessionContext.register_logger(LOGGER_NAME)
    api_url = os.environ['INGEST_API']
    ingest_api = IngestApi(api_url)
    ingest_service = IngestService(ingest_api)
    schema_service = SchemaService(ingest_api)
    metadata_service = MetadataService(ingest_client=ingest_api)


    gcp_config = GcpConfig.from_env()
    gcs_storage = GcsStorage(gcp_config.gcp_project, gcp_config.gcp_credentials_path, LOGGER_NAME)
    terra_config = TerraConfig.from_env()
    terra_client = TerraStorageClient(gcs_storage, schema_service, terra_config.terra_bucket_name,
                                      terra_config.terra_bucket_prefix, LOGGER_NAME)
    crawler = GraphCrawler(metadata_service=metadata_service)
    exporter = TerraExperimentExporter(ingest_service,crawler,terra_client, LOGGER_NAME)
    try:
        logger.info(f'Exporting metadata for process: {process_uuid}')
        exporter.export(process_uuid)
        logger.info(f'finished process: {process_uuid}')
    except Exception as e:
        logger.error(f'Error: reexporting {process_uuid}, {str(e)}', e)


if __name__ == '__main__':
    submission_uuid = sys.argv[1]
    num_threads = int(sys.argv[2]) if len(sys.argv) == 3 else 5  # Default to 5 threads
    api_url = os.environ['INGEST_API']
    ingest_api = IngestApi(api_url)
    assay_uuids = get_all_assay_processes_uuids("d0e49709-adff-4177-b01c-d6f384228730", ingest_api)
    with ThreadPool(5) as p:
        p.map(export_processes, assay_uuids)

