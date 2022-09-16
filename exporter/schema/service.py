from typing import Optional

from cachetools.func import ttl_cache
from hca_ingest.api.ingestapi import IngestApi

from exporter.schema.exceptions import SchemaParseException
from exporter.schema.resource import SchemaResource


class SchemaService:

    def __init__(self, ingest_client: IngestApi, ttl: Optional[int] = None):
        self.ingest_client = ingest_client
        self.ttl = ttl if ttl is not None else 300

        self.cached_latest_links_schema = ttl_cache(ttl=self.ttl)(self.latest_links_schema)
        self.cached_latest_file_descriptor_schema = ttl_cache(ttl=self.ttl)(self.latest_file_descriptor_schema)

    def latest_links_schema(self) -> SchemaResource:
        latest_schema = self.ingest_client.get_schemas(
            latest_only=True,
            high_level_entity="system",
            domain_entity="",
            concrete_entity="links"
        )[0]

        return SchemaResource.from_dict(latest_schema)

    def latest_file_descriptor_schema(self) -> SchemaResource:
        try:
            latest_schema = self.ingest_client.get_schemas(
                latest_only=True,
                high_level_entity="system",
                domain_entity="",
                concrete_entity="file_descriptor"
            )[0]

            return SchemaResource.from_dict(latest_schema)
        except IndexError as e:
            raise SchemaParseException(f'Failed to find latest file_descriptor schema')
