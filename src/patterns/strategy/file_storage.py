import logging
from io import BytesIO
from pathlib import Path

import pandas as pd
from minio import S3Error, Minio

from src.clients import MinioClient
from src.configurations import StorageConfiguration


class FileStorageStrategy:
    def read_data(self, input_file_name: Path) -> pd.DataFrame:
        raise NotImplementedError

    def save_data(self, df: pd.DataFrame, output_file_name: Path) -> pd.DataFrame:
        raise NotImplementedError


class LocalFileStorage(FileStorageStrategy):

    def read_data(self, input_file_name: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(StorageConfiguration.INPUT_DIRECTORY_PATH / input_file_name)
        except FileNotFoundError as e:
            logging.error(f"File not found: {StorageConfiguration.INPUT_DIRECTORY_PATH / input_file_name}, {e}")
            raise
        except Exception as e:
            logging.error(f"Failed to read data from local storage: {StorageConfiguration.INPUT_DIRECTORY_PATH} {e}")
            raise

    def save_data(self, df: pd.DataFrame, output_file_name: Path) -> pd.DataFrame:
        try:
            df.to_csv(StorageConfiguration.OUTPUT_DIRECTORY_PATH / output_file_name, index=False)
            logging.info(f"Saved data to local storage: {StorageConfiguration.OUTPUT_DIRECTORY_PATH  / output_file_name}")
            return df
        except Exception as e:
            logging.error(f"Failed to save data to local storage: {StorageConfiguration.OUTPUT_DIRECTORY_PATH  / output_file_name} {e}")
            raise


class MinioFileStorage(FileStorageStrategy):

    def read_data(self, input_file_name: str) -> pd.DataFrame:
        try:
            minio_client: Minio = MinioClient().connect()
            csv_bytes: bytes = minio_client.get_object(StorageConfiguration.MINIO_SOURCE_BUCKET_NAME, input_file_name).read()
            csv_buffer: BytesIO = BytesIO(csv_bytes)
            return pd.read_csv(csv_buffer)
        except S3Error as e:
            logging.error(f"Failed to read data from MinIO bucket {StorageConfiguration.MINIO_SOURCE_BUCKET_NAME}: {e}")
            raise

    def save_data(self, df: pd.DataFrame, output_file_name: str) -> pd.DataFrame:
        try:
            minio_client: Minio = MinioClient().client
            csv_bytes: bytes = df.to_csv(index=False).encode('utf-8')
            csv_buffer: BytesIO = BytesIO(csv_bytes)
            minio_client.put_object(
                bucket_name=StorageConfiguration.MINIO_DESTINATION_BUCKET_NAME,
                object_name=output_file_name,
                data=csv_buffer,
                length=len(csv_bytes),
                content_type='text/csv'
            )
            logging.info(f"Saved data to MinIO bucket: {StorageConfiguration.MINIO_DESTINATION_BUCKET_NAME}")
            return df
        except S3Error as e:
            logging.error(f"Failed to save data to MinIO bucket {StorageConfiguration.MINIO_DESTINATION_BUCKET_NAME}: {e}")
            raise
