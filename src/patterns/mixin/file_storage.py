from pathlib import Path

import pandas as pd
from minio import Minio

from src.clients import MinioClient
from src.config import Config
from src.patterns.strategy.file_storage import LocalFileStorage, MinioFileStorage


class FileStorageMixin:
    def __init__(self):
        if Config.FILE_STORAGE_TYPE == 'LOCAL':
            if not Config.OUTPUT_DIRECTORY_PATH.exists():
                Config.OUTPUT_DIRECTORY_PATH.mkdir(parents=True)
            self.file_storage_strategy = LocalFileStorage()
        elif Config.FILE_STORAGE_TYPE == 'MINIO':
            minio_client: Minio = MinioClient().connect()
            if not minio_client.bucket_exists(Config.MINIO_DESTINATION_BUCKET_NAME):
                minio_client.make_bucket(Config.MINIO_DESTINATION_BUCKET_NAME)
            self.file_storage_strategy = MinioFileStorage()
        else:
            raise ValueError(f"Unsupported storage type: {Config.FILE_STORAGE_TYPE}")

    @classmethod
    def get_input_file_location(cls) -> str | None | Path:
        if Config.FILE_STORAGE_TYPE == 'LOCAL':
            return Config.INPUT_DIRECTORY_PATH
        elif Config.FILE_STORAGE_TYPE == 'MINIO':
            return Config.MINIO_SOURCE_BUCKET_NAME

    @classmethod
    def get_output_file_location(cls) -> str | None | Path:
        if Config.FILE_STORAGE_TYPE == 'LOCAL':
            return Config.OUTPUT_DIRECTORY_PATH
        elif Config.FILE_STORAGE_TYPE == 'MINIO':
            return Config.MINIO_DESTINATION_BUCKET_NAME

    def read_data(self, input_file_location: Path | str, input_file_name: Path, column_order: list[str] = None,
                  drop_duplicates: bool = False) -> pd.DataFrame:
        df: pd.DataFrame = self.file_storage_strategy.read_data(input_file_location, input_file_name)
        if column_order:
            df = df[column_order]
        if drop_duplicates:
            df = df.drop_duplicates()
        return df

    def save_data(self, df: pd.DataFrame, output_file_location: Path | str, output_file_name: Path,
                  column_order: list[str],
                  drop_duplicates: bool = False,
                  drop_na: bool = False) -> pd.DataFrame:
        column_order = [col for col in column_order if col in df.columns]
        df_copy = df.copy()[column_order]
        if drop_duplicates:
            df_copy = df_copy.drop_duplicates()
        if drop_na:
            df_copy = df_copy.dropna()
        self.file_storage_strategy.save_data(df_copy, output_file_location, output_file_name)
        return df
