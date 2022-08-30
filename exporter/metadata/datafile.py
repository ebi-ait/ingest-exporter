from dataclasses import dataclass

from exporter.metadata.checksums import FileChecksums
from exporter.metadata.exceptions import MetadataParseException
from exporter.metadata.resource import MetadataResource


@dataclass
class DataFile:
    uuid: str
    dcp_version: str
    file_name: str
    cloud_url: str
    content_type: str
    size: int
    checksums: FileChecksums

    def source_bucket(self) -> str:
        return self.cloud_url.split("//")[1].split("/")[0]

    def source_key(self) -> str:
        return self.cloud_url.split("//")[1].split("/", 1)[1]

    @staticmethod
    def from_file_metadata(file_metadata: MetadataResource) -> 'DataFile':
        if file_metadata.full_resource is not None:
            try:
                return DataFile(file_metadata.full_resource["dataFileUuid"],
                                file_metadata.dcp_version,
                                file_metadata.full_resource["fileName"],
                                file_metadata.full_resource["cloudUrl"],
                                file_metadata.full_resource["fileContentType"],
                                file_metadata.full_resource["size"],
                                FileChecksums.from_dict(file_metadata.full_resource["checksums"]))
            except (KeyError, TypeError) as e:
                raise MetadataParseException(e)
        else:
            raise MetadataParseException(f'Error: parsing DataFile from file MetadataResources requires non-empty'
                                         f'"full_resource" field. Metadata:\n\n {file_metadata}')
