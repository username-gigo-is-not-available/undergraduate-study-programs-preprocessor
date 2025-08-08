import json
import logging
from io import BytesIO
from pathlib import Path
from typing import  Any

import pandas as pd
from fastavro import validate, writer, parse_schema, reader
from minio import S3Error, Minio
from urllib3 import BaseHTTPResponse

from src.clients import MinioClient
from src.configurations import StorageConfiguration


class StorageStrategy:

    def load_schema(self, schema_file_name: Path):
        raise NotImplementedError

    def read_data(self, input_file_name: Path) -> pd.DataFrame:
        raise NotImplementedError

    def save_data(self, df: pd.DataFrame, output_file_name: Path, schema_file_name: Path) -> pd.DataFrame:
        raise NotImplementedError

    @classmethod
    def serialize(cls, data: pd.DataFrame, schema: dict) -> BytesIO:
        buffer = BytesIO()
        writer(buffer, schema, [record for record in data.to_dict('records')])
        buffer.seek(0)
        return buffer


class LocalStorage(StorageStrategy):

    def load_schema(self, schema_file_name: Path) -> str | list | dict | None:
        path: Path = StorageConfiguration.SCHEMA_DIRECTORY_PATH / schema_file_name
        try:
            logging.info(f"Reading schema from local storage: {path}")
            with open(path, "r", encoding="utf-8") as f:
                raw: dict = json.load(f)
                return parse_schema(raw)
        except OSError as e:
            logging.error(f"Failed to read schema from local storage: {path} {e}")
            return {}

    def read_data(self, input_file_name: Path) -> pd.DataFrame:
        path: Path = StorageConfiguration.INPUT_DATA_DIRECTORY_PATH / input_file_name
        try:
            with open(path, "rb") as f:
                records: list[dict[str, Any]] = list(reader(f))
            return pd.DataFrame(records)
        except FileNotFoundError as e:
            logging.error(f"File not found: {path}, {e}")
            return pd.DataFrame()
        except OSError as e:
            logging.error(f"Failed to read data from local storage {path}: {e}")
            return pd.DataFrame()

    def save_data(self, df: pd.DataFrame, output_file_name: Path, schema_file_name: Path) -> pd.DataFrame:
        path: Path = StorageConfiguration.OUTPUT_DATA_DIRECTORY_PATH / output_file_name
        try:
            schema: dict =  self.load_schema(schema_file_name)
            buffer: BytesIO = self.serialize(df, schema)
            with open(path, "wb") as f:
                f.write(buffer.read())
            return df
        except Exception as e:
            logging.error(f"Failed to save data to local storage {path}: {e}")
            return pd.DataFrame()


class MinioStorage(StorageStrategy):

    def load_schema(self, schema_file_name: Path) -> dict:
        object_name: str = "/".join([StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME, str(schema_file_name)])
        try:
            logging.info(f"Reading schema from MinIO bucket: {StorageConfiguration.MINIO_SCHEMA_BUCKET_NAME}/{object_name}")
            minio: Minio = MinioClient.connect()
            response: BaseHTTPResponse = minio.get_object(
                    bucket_name=StorageConfiguration.MINIO_SCHEMA_BUCKET_NAME,
                    object_name=object_name,
                )
            data: bytes = response.read()
            buffer: BytesIO = BytesIO(data)
            return parse_schema(json.load(buffer))
        except S3Error as e:
            logging.error(
                f"Failed to read schema from MinIO bucket {StorageConfiguration.MINIO_SCHEMA_BUCKET_NAME}/{object_name}: {e}")
            return {}


    def read_data(self, input_file_name: Path) -> pd.DataFrame:
        try:
            input_file_name: str = str(input_file_name)
            minio: Minio = MinioClient().connect()
            data: bytes = minio.get_object(StorageConfiguration.MINIO_INPUT_DATA_BUCKET_NAME, input_file_name).read()
            buffer: BytesIO = BytesIO(data)
            return pd.DataFrame(list(reader(buffer)))
        except S3Error as e:
            logging.error(
                f"Failed to read data from MinIO bucket {StorageConfiguration.MINIO_INPUT_DATA_BUCKET_NAME}: {e}")
            return pd.DataFrame()

    def save_data(self, df: pd.DataFrame, output_file_name: str, schema_file_name: Path) -> pd.DataFrame:
        try:
            schema: dict =  self.load_schema(schema_file_name)
            output_file_name: str = str(output_file_name)
            minio: Minio = MinioClient().client
            buffer: BytesIO = self.serialize(df, schema)
            minio.put_object(
                bucket_name=StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME,
                object_name=output_file_name,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type='application/avro'
            )
            logging.info(f"Saved data to MinIO bucket: {StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME}")
            return df
        except S3Error as e:
            logging.error(
                f"Failed to save data to MinIO bucket {StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME}: {e}")
            return pd.DataFrame()
