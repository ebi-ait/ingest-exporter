from dataclasses import dataclass
from typing import Dict

from exporter.metadata.checksums import FileChecksums
from exporter.metadata.datafile import DataFile
from exporter.metadata.resource import MetadataResource


@dataclass
class FileDescriptor:
    file_uuid: str
    file_version: str
    file_name: str
    content_type: str
    size: int
    checksums: FileChecksums

    def to_dict(self) -> Dict:
        sha1 = self.checksums.sha1.lower() if self.checksums.sha1 else self.checksums.sha1
        sha256 = self.checksums.sha256.lower() if self.checksums.sha256 else self.checksums.sha256
        crc32c = str(self.checksums.crc32c).lower() if self.checksums.crc32c else self.checksums.crc32c

        return dict(
            file_id=self.file_uuid,
            file_version=self.file_version,
            file_name=self.file_name,
            content_type=self.content_type,
            size=self.size,
            sha1=sha1,
            sha256=sha256,
            crc32c=crc32c,
            s3_etag=self.checksums.s3_etag,
            schema_type='file_descriptor'
        )

    @staticmethod
    def from_file_metadata(file_metadata: MetadataResource) -> 'FileDescriptor':
        data_file = DataFile.from_file_metadata(file_metadata)
        return FileDescriptor(data_file.uuid, file_metadata.dcp_version, data_file.file_name,
                              data_file.content_type, data_file.size, data_file.checksums)
