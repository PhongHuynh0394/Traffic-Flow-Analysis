from airflow.hooks.base import BaseHook
from dotenv import load_dotenv
import os
import json
from google.cloud import storage
from google.oauth2 import service_account
import logging

class GCSHook(BaseHook):

    def __init__(self, 
                 bucket: str,
                 gcs_credential_env: str = "GOOGLE_APPLICATION_CREDENTIALS",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.gcs_credential_env = gcs_credential_env
        self.client = self._get_gcs_client()
        self.bucket = self.client.bucket(bucket)


    def _get_gcs_client(self):
        gcs_env = os.getenv(self.gcs_credential_env)
        if not gcs_env:
            raise ValueError(f"Environment variable {self.gcs_credential_env} is not set.")
        credential_info = json.loads(gcs_env)
        credentials = service_account.Credentials.from_service_account_info(credential_info)
        return storage.Client(credentials=credentials, project=credential_info.get("project_id"))
    

    def upload_file(self, file_path, destination_blob_name):
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        self.log.info(f"Uploaded {file_path} to gs://{self.bucket_name}/{destination_blob_name}")


    def download_file(self, source_blob_name, destination_file_name):
        blob = self.bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        self.log.info(f"Downloaded gs://{self.bucket_name}/{source_blob_name} to {destination_file_name}")
    

    def delete_file(self, blob_name):
        blob = self.bucket.blob(blob_name)
        try:
            blob.delete()
            self.log.info(f"Deleted gs://{self.bucket_name}/{blob_name}")
        except Exception as e:
            self.log.error(f"Error deleting gs://{self.bucket_name}/{blob_name}: {e}")
    
    
    def is_file_exists(self, blob_name):
        blob = self.bucket.blob(blob_name)
        return blob.exists()

    def list_blobs(self, prefix=None):
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        return blobs
    
    def upload_bytes(self, data: bytes, destination_blob_name: str, content_type: str = "application/octet-stream"):
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(data, content_type=content_type)
        self.log.info(f"Uploaded image bytes to gs://{self.bucket.name}/{destination_blob_name}")
