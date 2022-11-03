from exporter.schema.exceptions import SchemaParseException


class SchemaResource:

    def __init__(self, schema_url: str, schema_version: str):
        self.schema_url = schema_url
        self.schema_version = schema_version

    @staticmethod
    def from_dict(data: dict) -> 'SchemaResource':
        try:
            schema_url = data["_links"]["json-schema"]["href"]
            schema_version = data["schemaVersion"]
            return SchemaResource(schema_url, schema_version)
        except (KeyError, TypeError) as e:
            raise SchemaParseException(e) from e
