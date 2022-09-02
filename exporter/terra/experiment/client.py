import json
import logging
from io import StringIO
from typing import Iterable, Dict, Tuple

import requests
from google.cloud.storage import Client
from google.oauth2.service_account import Credentials
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate

from exporter.graph.link_set import LinkSet
from exporter.metadata.descriptor import FileDescriptor
from exporter.metadata.exceptions import MetadataParseException
from exporter.metadata.resource import MetadataResource
from exporter.schema.service import SchemaService
from exporter.terra.gcs.storage import Streamable, GcsStorage
from exporter.utils import log_function_and_params

LOGGER_NAME = 'TerraExperimentExporter'


def storage_client_from_gcs_info(
        service_account_credentials_path: str,
        gcp_project: str,
        bucket_name: str,
        bucket_prefix: str
):
    with open(service_account_credentials_path) as source:
        info = json.load(source)
    storage_credentials: Credentials = Credentials.from_service_account_info(info)
    gcs_client = Client(project=gcp_project, credentials=storage_credentials)
    return GcsStorage(gcs_client, bucket_name, bucket_prefix)


class TerraStorageClient:
    def __init__(self, gcs_storage: GcsStorage, schema_service: SchemaService):
        self.gcs_storage = gcs_storage
        self.schema_service = schema_service
        self.logger = logging.getLogger(LOGGER_NAME)

    @log_function_and_params(logging.getLogger(LOGGER_NAME))
    def write_metadatas(self, metadatas: Iterable[MetadataResource], project_uuid: str):
        for metadata in metadatas:
            self.write_metadata(metadata, project_uuid)

    @log_function_and_params(logging.getLogger(LOGGER_NAME))
    def write_metadata(self, metadata: MetadataResource, project_uuid: str):
        # TODO1: only proceed if lastContentModified > last
        dest_object_key = f'{project_uuid}/metadata/{metadata.concrete_type()}/{metadata.uuid}_{metadata.dcp_version}.json'

        metadata_json = metadata.get_content(with_provenance=True)
        data_stream = self.dict_to_json_stream(metadata_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

        # TODO2: patch dcpVersion        
        #patch_url = metadata.metadata_json['_links']['self']['href']
        #self.ingest_client.patch(patch_url, {"dcpVersion": metadata.dcp_version})

        self.logger.info(f'Writing metadata for type: {metadata.metadata_type}')
        if metadata.metadata_type == "file":
            self.write_file_descriptor(metadata, project_uuid)

    def write_links(self, link_set: LinkSet, process_uuid: str, process_version: str, project_uuid: str):
        dest_object_key = f'{project_uuid}/links/{process_uuid}_{process_version}_{project_uuid}.json'
        links_json = self.generate_links_json(link_set)
        data_stream = self.dict_to_json_stream(links_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

    @log_function_and_params(logging.getLogger(LOGGER_NAME))
    def write_file_descriptor(self, file_metadata: MetadataResource, project_uuid: str):
        dest_object_key = f'{project_uuid}/descriptors/{file_metadata.concrete_type()}/{file_metadata.uuid}_{file_metadata.dcp_version}.json'
        file_descriptor_json = self.generate_file_descriptor_json(file_metadata)
        self.logger.info(f'Writing file descriptor with dataFileUuid: {file_descriptor_json.get("file_id")}, '
                         f'projectUuid: {project_uuid}')
        data_stream = self.dict_to_json_stream(file_descriptor_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

    def generate_file_descriptor_json(self, file_metadata) -> Dict:
        file_descriptor = FileDescriptor.from_file_metadata(file_metadata)

        json_doc = file_descriptor.to_dict()
        latest_schema = self.schema_service.cached_latest_file_descriptor_schema()
        self.update_schema_info_and_validate(json_doc, latest_schema)
        return json_doc

    def write_to_staging_bucket(self, object_key: str, data_stream: Streamable):
        self.gcs_storage.write(object_key, data_stream)

    def generate_links_json(self, link_set: LinkSet) -> Dict:
        json_doc = link_set.to_dict()
        json_doc["schema_type"] = "links"
        latest_schema = self.schema_service.cached_latest_links_schema()
        self.update_schema_info_and_validate(json_doc, latest_schema)
        return json_doc

    def write_staging_area_json(self, project_uuid: str):
        dest_object_key = f'{project_uuid}/staging_area.json'
        data_stream = self.dict_to_json_stream({'is_delta': False})
        self.write_to_staging_bucket(dest_object_key, data_stream)

    @staticmethod
    def dict_to_json_stream(d: Dict) -> StringIO:
        return StringIO(json.dumps(d))

    @staticmethod
    def bucket_and_key_for_upload_area(upload_area: str) -> Tuple[str, str]:
        bucket_and_key_str = upload_area.split("//")[1]
        bucket_and_key_list = bucket_and_key_str.split("/", 1)

        return bucket_and_key_list[0], bucket_and_key_list[1].split("/")[0]

    @staticmethod
    def update_schema_info_and_validate(json_doc, json_schema):
        json_doc["describedBy"] = json_schema.schema_url
        json_doc["schema_version"] = json_schema.schema_version
        json_schema = requests.get(json_doc["describedBy"]).json()
        try:
            validate(instance=json_doc, schema=json_schema)
        except ValidationError as e:
            raise MetadataParseException(f'problem validating document {json_doc} with schema: {json_doc["describedBy"]}', e)