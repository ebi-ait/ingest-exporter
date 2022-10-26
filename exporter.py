#!/usr/bin/env python
import os

from exporter.terra.experiment.config import setup_terra_experiment_exporter
from exporter.terra.spreadsheet.config import setup_terra_spreadsheet_exporter
from exporter.terra.submission.config import setup_terra_submissions_exporter
from manifest.config import setup_manifest_receiver

DISABLE_MANIFEST = os.environ.get('DISABLE_MANIFEST', False)
if __name__ == '__main__':
    manifest_thread = None
    if not DISABLE_MANIFEST:
        manifest_thread = setup_manifest_receiver()

    terra_submissions_thread, terra_transfer_response_thread = setup_terra_submissions_exporter()
    terra_spreadsheet_thread = setup_terra_spreadsheet_exporter()
    terra_experiment_thread = setup_terra_experiment_exporter()

    if not DISABLE_MANIFEST:
        manifest_thread.join()
    terra_submissions_thread.join()
    terra_spreadsheet_thread.join()
    terra_transfer_response_thread.join()
    terra_experiment_thread.join()
