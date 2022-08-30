from dataclasses import dataclass
from typing import Dict

from exporter.scratch import MetadataParseException


@dataclass
class FileChecksums:
    sha256: str
    crc32c: str
    sha1: str
    s3_etag: str

    @staticmethod
    def from_dict(data: Dict) -> 'FileChecksums':
        try:
            sha256 = data["sha256"]
            crc32c = data["crc32c"]
            sha1 = data["sha1"]
            s3_etag = data["s3_etag"]

            return FileChecksums(sha256, crc32c, sha1, s3_etag)
        except (KeyError, TypeError) as e:
            raise MetadataParseException(e)
