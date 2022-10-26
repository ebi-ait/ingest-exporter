import os
from dataclasses import dataclass


@dataclass
class GcpConfig:
    gcp_credentials_path:str
    gcp_project:str
    gcp_topic:str

    @staticmethod
    def from_env():
        gcp_credentials_path = os.environ['GCP_SVC_ACCOUNT_KEY_PATH']
        gcp_project = os.environ['GCP_PROJECT']
        gcp_topic = os.environ['TERRA_GCS_NOTIFICATION_TOPIC']
        return GcpConfig(gcp_credentials_path, gcp_project, gcp_topic)
