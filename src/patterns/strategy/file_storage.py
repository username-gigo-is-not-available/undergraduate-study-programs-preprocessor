import logging
from io import BytesIO
from pathlib import Path

import pandas as pd
from minio import S3Error, Minio

from src.clients import MinioClient


class FileStorageStrategy:
    def read_data(self, input_file_location: Path | str, input_file_name: Path) -> pd.DataFrame:
        raise NotImplementedError

    def save_data(self, df: pd.DataFrame, output_file_location: Path | str, output_file_name: Path) -> pd.DataFrame:
        raise NotImplementedError


class LocalFileStorage(FileStorageStrategy):

    def read_data(self, input_file_location: Path, input_file_name: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(input_file_location / input_file_name)
        except FileNotFoundError as e:
            logging.error(f"File not found: {input_file_location / input_file_name}, {e}")
            raise
        except Exception as e:
            logging.error(f"Failed to read data from local storage: {input_file_location} {e}")
            raise

    def save_data(self, df: pd.DataFrame, output_file_location: Path, output_file_name: Path) -> pd.DataFrame:
        try:
            df.to_csv(output_file_location / output_file_name, index=False)
            logging.info(f"Saved data to local storage: {output_file_location / output_file_name}")
            return df
        except Exception as e:
            logging.error(f"Failed to save data to local storage: {output_file_location / output_file_name} {e}")
            raise


class MinioFileStorage(FileStorageStrategy):

    def read_data(self,input_file_location: str, input_file_name: Path) -> pd.DataFrame:
        try:
            minio_client: Minio = MinioClient().client
            csv_bytes: bytes = minio_client.get_object(input_file_location, str(input_file_name)).read()
            csv_buffer: BytesIO = BytesIO(csv_bytes)
            return pd.read_csv(csv_buffer)
        except S3Error as e:
            logging.error(f"Failed to read data from MinIO bucket {input_file_location}: {e}")
            raise

    def save_data(self, df: pd.DataFrame, output_file_location: str, output_file_name: Path) -> pd.DataFrame:
        try:
            minio_client: Minio = MinioClient().client
            csv_bytes: bytes = df.to_csv(index=False).encode('utf-8')
            csv_buffer: BytesIO = BytesIO(csv_bytes)
            minio_client.put_object(
                bucket_name=output_file_location,
                object_name=str(output_file_name),
                data=csv_buffer,
                length=len(csv_bytes),
                content_type='text/csv'
            )
            logging.info(f"Saved data to MinIO bucket: {output_file_location}")
            return df
        except S3Error as e:
            logging.error(f"Failed to save data to MinIO bucket {output_file_location}: {e}")
            raise
