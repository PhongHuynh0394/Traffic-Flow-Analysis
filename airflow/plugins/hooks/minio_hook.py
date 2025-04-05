import minio
from airflow.hooks.base_hook import BaseHook
from minio import Minio
from minio.error import S3Error

class MinioHook(BaseHook):
    def __init__(self, conn_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.client = self._get_minio_client()


    def _get_minio_client(self):
        connection = self.get_connection(self.conn_id)
        return Minio(
            connection.host,
            access_key=connection.login,
            secret_key=connection.password,
            secure=False  
        )


    def upload_file(self, bucket_name: str, file_path: str, object_name: str):
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            self.log.info(f"File {file_path} uploaded successfully to {bucket_name}/{object_name}")
        except S3Error as e:
            self.log.error(f"Error uploading file to MinIO: {e}")
            raise


    def download_file(self, bucket_name: str, object_name: str, file_path: str):
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            self.log.info(f"File {object_name} downloaded successfully to {file_path}")
        except S3Error as e:
            self.log.error(f"Error downloading file from MinIO: {e}")
            raise


    def list_objects(self, bucket_name: str):
        try:
            objects = self.client.list_objects(bucket_name)
            return [obj.object_name for obj in objects]
        except S3Error as e:
            self.log.error(f"Error listing objects in MinIO bucket: {e}")
            raise
