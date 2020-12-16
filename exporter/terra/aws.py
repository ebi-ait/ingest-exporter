from boto3 import Session
import json

from botocore.exceptions import ClientError


class AwsStorage:
    def __init__(self, session: Session, bucket_name: str, storage_prefix: str):
        self.session = session
        self.client = self.session.client('s3')
        self.bucket_name = bucket_name
        self.storage_prefix = storage_prefix

    def write_json(self, key: str, json_data: dict):
        dest_key = f'{self.storage_prefix}/{key}'
        self.client.put_object(Bucket=bucket_name, Key=dest_key,
                               Body=json.dumps(json_data, indent=2),
                               ContentType='application/json')

    def copy_file(self, source_bucket, source_key, dest_key):
        dest_bucket = self.bucket_name
        dest_key = f'{self.storage_prefix}/{dest_key}'
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }

        self.client.copy(copy_source, dest_bucket, dest_key)

    def file_exists(self, bucket, key) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as error:
            if error.response['Error']['Code'] != '404':
                return False
