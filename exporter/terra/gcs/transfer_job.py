from dataclasses import dataclass
from datetime import datetime
from typing import Dict

from google.cloud.pubsub_v1 import SubscriberClient


@dataclass
class TransferJob:
    name: str
    description: str
    project_id: str
    source_bucket: str
    source_path: str
    aws_access_key_id: str
    aws_access_key_secret: str
    dest_bucket: str
    dest_path: str
    notification_topic: str

    def __post_init__(self):
        self.notification_topic = SubscriberClient.topic_path(self.project_id, self.notification_topic)

    def to_dict(self) -> Dict:
        start_date = datetime.now()
        return {
            'name': self.name,
            'description': self.description,
            'status': 'ENABLED',
            'projectId': self.project_id,
            'schedule': {
                'scheduleStartDate': {
                    'day': start_date.day,
                    'month': start_date.month,
                    'year': start_date.year
                },
                'scheduleEndDate': {
                    'day': start_date.day,
                    'month': start_date.month,
                    'year': start_date.year
                }
            },
            'transferSpec': {
                'awsS3DataSource': {
                    'bucketName': self.source_bucket,
                    'awsAccessKey': {
                        'accessKeyId': self.aws_access_key_id,
                        'secretAccessKey': self.aws_access_key_secret
                    },
                    'path': self.source_path
                },
                'gcsDataSink': {
                    'bucketName': self.dest_bucket,
                    'path': self.dest_path
                },
                'transferOptions': {
                    'overwriteObjectsAlreadyExistingInSink': False
                }
            },
            'notificationConfig': {
                'pubsubTopic': self.notification_topic,
                'eventTypes': ['TRANSFER_OPERATION_SUCCESS'],
                'payloadFormat': 'JSON'
            }
        }
