import logging
import unittest
from unittest.mock import patch

from ingest.api.ingestapi import IngestApi

from exporter.graph.graph_crawler import GraphCrawler
from exporter.metadata import MetadataService
from exporter.session_context import configure_logger
from exporter.terra.dcp_staging_client import DcpStagingClient
from exporter.terra.terra_export_job import TerraExportJobService
from exporter.terra.terra_exporter import TerraExporter


class TerraExporterTestCase(unittest.TestCase):

    # patching the 5 dependencies of TerraExporter
    @patch('exporter.terra.terra_export_job.TerraExportJobService')
    @patch('exporter.terra.dcp_staging_client.DcpStagingClient')
    @patch('exporter.graph.graph_crawler.GraphCrawler')
    @patch('exporter.metadata.MetadataService')
    @patch('ingest.api.ingestapi.IngestApi')
    def test_log_records_have_submission_uuid(self,
                                     ingest_client: IngestApi,
                                     metadata_service: MetadataService,
                                     graph_crawler: GraphCrawler,
                                     dcp_staging_client: DcpStagingClient,
                                     job_service: TerraExportJobService):
        configure_logger(logging.getLogger())
        exporter = TerraExporter(ingest_client,
                                 metadata_service,
                                 graph_crawler,
                                 dcp_staging_client,
                                 job_service)
        process_uuid = 'test_pid'
        submission_uuid = 'test_sid'
        export_job_id = 'test_jid'
        # The call to export() is surrounded with try catch because it throws an exception.
        # The purpose of the test is to verify the LogRecord has been modified to include
        # the submission id.
        try:
            exporter.export(process_uuid, submission_uuid, export_job_id)
        except:
            pass
        self.fail('xxx')

if __name__ == '__main__':
    unittest.main()
