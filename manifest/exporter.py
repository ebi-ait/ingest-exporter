import logging

from hca_ingest.api.ingestapi import IngestApi

from manifest.generator import ManifestGenerator


class ManifestExporter:
    def __init__(self, ingest_api: IngestApi, manifest_generator: ManifestGenerator):
        self.logger = logging.getLogger('ManifestExporter')
        self.ingest_api = ingest_api
        self.manifest_generator = manifest_generator

    def export(self, process_uuid: str, submission_uuid: str):
        assay_manifest = self.manifest_generator.generate_manifest(process_uuid, submission_uuid)
        assay_manifest_resource = self.ingest_api.create_bundle_manifest(assay_manifest)
        assay_manifest_url = assay_manifest_resource['_links']['self']['href']
        self.logger.info(f"Assay manifest was created: {assay_manifest_url}")
