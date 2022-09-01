#!/usr/bin/env python
import logging
import os

from exporter.session_context import configure_logger
from exporter.terra.experiment.config import setup_terra_experiment_exporter
from exporter.terra.submission.config import setup_terra_submissions_exporter
from manifest.config import setup_manifest_receiver

DISABLE_MANIFEST = os.environ.get('DISABLE_MANIFEST', False)
if __name__ == '__main__':
    configure_logger(logging.getLogger(''))
    ingest_logger = logging.getLogger('ingest')
    ingest_logger.setLevel(logging.INFO)
    manifest_logger = logging.getLogger('manifest')
    manifest_logger.setLevel(logging.INFO)

    manifest_thread = None
    if not DISABLE_MANIFEST:
        manifest_thread = setup_manifest_receiver()

    terra_submissions_thread = setup_terra_submissions_exporter()
    terra_experiment_thread = setup_terra_experiment_exporter()

    if not DISABLE_MANIFEST:
        manifest_thread.join()
    terra_submissions_thread.join()
    terra_experiment_thread.join()
