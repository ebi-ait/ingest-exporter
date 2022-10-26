import os
from dataclasses import dataclass


@dataclass
class TerraConfig:
    terra_bucket_name: str
    terra_bucket_prefix: str

    @staticmethod
    def from_env():
        terra_bucket_name = os.environ['TERRA_BUCKET_NAME']
        terra_bucket_prefix = os.environ['TERRA_BUCKET_PREFIX']
        return TerraConfig(terra_bucket_name, terra_bucket_prefix)
