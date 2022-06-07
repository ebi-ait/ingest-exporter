import logging
import unittest
from unittest.mock import patch

from ingest.api.ingestapi import IngestApi

from exporter.graph.graph_crawler import GraphCrawler
from exporter.metadata import MetadataService
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
    # patching the log handler to be able to verify the LogRecord has been modified to contain
    # the submission_uuid.
    # This is not the most elegant way to test this functionality.
    @patch.object(logging.StreamHandler, 'emit', wraps=logging.StreamHandler.emit)
    def test_log_records_have_submission_uuid(self,
                                     mock_log_handler,
                                     ingest_client: IngestApi,
                                     metadata_service: MetadataService,
                                     graph_crawler: GraphCrawler,
                                     dcp_staging_client: DcpStagingClient,
                                     job_service: TerraExportJobService):
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
        self.verify_log_record(mock_log_handler, key='submission_uuid', value=submission_uuid)

    def verify_log_record(self, mock_log_handler, key, value):
        log_record = mock_log_handler.call_args.args[0]
        self.assertIn(key, log_record.__dict__)
        self.assertEquals(getattr(log_record, key), value)


if __name__ == '__main__':
    unittest.main()
