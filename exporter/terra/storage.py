import json
import logging
from io import StringIO
from typing import Iterable, Dict, Tuple

import requests
from jsonschema.exceptions import ValidationError
from jsonschema.validators import validate as validate_against_schema

from exporter.graph.link.link_set import LinkSet
from exporter.metadata.descriptor import FileDescriptor
from exporter.metadata.exceptions import MetadataParseException
from exporter.metadata.resource import MetadataResource
from exporter.schema.service import SchemaService

from .gcs.storage import Streamable, GcsStorage


class TerraStorageClient:
    def __init__(self, gcs_storage: GcsStorage, schema_service: SchemaService, bucket_name: str,
                 key_prefix: str = '', logger_name: str = __name__):
        self.gcs_storage = gcs_storage
        self.schema_service = schema_service
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix
        self.logger = logging.getLogger(logger_name)

    def write_metadatas(self, metadatas: Iterable[MetadataResource], project_uuid: str):
        for metadata in metadatas:
            self.write_metadata(metadata, project_uuid)

    def write_metadata(self, metadata: MetadataResource, project_uuid: str):
        # TODO1: only proceed if lastContentModified > last
        dest_object_key = f'{project_uuid}/metadata/{metadata.concrete_type()}/{metadata.uuid}_{metadata.dcp_version}.json'

        metadata_json = metadata.get_content(with_provenance=True)
        data_stream = self.dict_to_json_stream(metadata_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

        # TODO2: patch dcpVersion        
        # patch_url = metadata.metadata_json['_links']['self']['href']
        # self.ingest_client.patch(patch_url, {"dcpVersion": metadata.dcp_version})

        if metadata.metadata_type == "file":
            self.write_file_descriptor(metadata, project_uuid)

    def write_links(self, link_set: LinkSet, process_uuid: str, process_version: str, project_uuid: str):
        dest_object_key = f'{project_uuid}/links/{process_uuid}_{process_version}_{project_uuid}.json'
        links_json = self.generate_links_json(link_set)
        data_stream = self.dict_to_json_stream(links_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

    def write_file_descriptor(self, file_metadata: MetadataResource, project_uuid: str):
        dest_object_key = f'{project_uuid}/descriptors/{file_metadata.concrete_type()}/{file_metadata.uuid}_{file_metadata.dcp_version}.json'
        file_descriptor_json = self.generate_file_descriptor_json(file_metadata)
        self.logger.info(f'Writing file descriptor with dataFileUuid: {file_descriptor_json.get("file_id")}')
        data_stream = self.dict_to_json_stream(file_descriptor_json)
        self.write_to_staging_bucket(dest_object_key, data_stream)

    def generate_file_descriptor_json(self, file_metadata) -> Dict:
        file_descriptor = FileDescriptor.from_file_metadata(file_metadata)

        json_doc = file_descriptor.to_dict()
        latest_schema = self.schema_service.cached_latest_file_descriptor_schema()
        TerraStorageClient.update_schema_info_and_validate(json_doc, latest_schema)
        return json_doc

    def write_to_staging_bucket(self, object_key: str, data_stream: Streamable, overwrite=False):
        file_key = f"{self.key_prefix}/{object_key}"
        self.logger.info(f'{"Overwriting" if overwrite else "Writing"} file: {file_key}')
        self.gcs_storage.write(self.bucket_name, file_key, data_stream, overwrite)

    def generate_links_json(self, link_set: LinkSet) -> Dict:
        json_doc = link_set.to_dict()
        json_doc["schema_type"] = "links"
        latest_schema = self.schema_service.cached_latest_links_schema()
        TerraStorageClient.update_schema_info_and_validate(json_doc, latest_schema)
        return json_doc

    def write_staging_area_json(self, project_uuid: str):
        dest_object_key = f'{project_uuid}/staging_area.json'
        data_stream = self.dict_to_json_stream({'is_delta': False})
        self.write_to_staging_bucket(dest_object_key, data_stream)

    def delete_metadata(self, project_uuid: str, metadata_type: str, concrete_type: str, metadata_uuid: str):
        meta_prefix = f'{self.key_prefix}/{project_uuid}/metadata/{concrete_type}/{metadata_uuid}'
        self.gcs_storage.delete_all(self.bucket_name, meta_prefix)
        links_prefix = f'{self.key_prefix}/{project_uuid}/links/{metadata_uuid}'
        self.gcs_storage.delete_all(self.bucket_name, links_prefix)
        if metadata_type == "file":
            descriptor_prefix = f'{self.key_prefix}/{project_uuid}/descriptors/{concrete_type}/{metadata_uuid}'
            self.gcs_storage.delete_all(self.bucket_name, descriptor_prefix)

    def delete_data_file(self, project_uuid: str, file_name: str):
        data_file_key = f'{self.key_prefix}/{project_uuid}/data/{file_name}'
        self.gcs_storage.delete(self.bucket_name, data_file_key)

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
        TerraStorageClient.validate_json_doc(json_doc)

    @staticmethod
    def validate_json_doc(json_doc, json_schema=None):
        if json_schema is None:
            json_schema = requests.get(json_doc["describedBy"]).json()
        try:
            validate_against_schema(instance=json_doc, schema=json_schema)
        except ValidationError as e:
            raise MetadataParseException(
                f'problem validating document: invalid json path \'{e.json_path}\', schema: {json_doc["describedBy"]}, document {json_doc}', e)
