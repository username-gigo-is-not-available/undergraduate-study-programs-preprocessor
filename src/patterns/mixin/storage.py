
import pandas as pd
from minio import Minio

from src.clients import MinioClient
from src.configurations import StorageConfiguration, DatasetConfiguration
from src.patterns.strategy.storage import LocalStorage, MinioStorage


class StorageMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if StorageConfiguration.FILE_STORAGE_TYPE == 'LOCAL':
            if not StorageConfiguration.OUTPUT_DATA_DIRECTORY_PATH.exists():
                StorageConfiguration.OUTPUT_DATA_DIRECTORY_PATH.mkdir(parents=True)
            self.storage_strategy = LocalStorage()
        elif StorageConfiguration.FILE_STORAGE_TYPE == 'MINIO':
            minio_client: Minio = MinioClient().connect()
            if not minio_client.bucket_exists(StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME):
                minio_client.make_bucket(StorageConfiguration.MINIO_OUTPUT_DATA_BUCKET_NAME)
            self.storage_strategy = MinioStorage()
        else:
            raise ValueError(f"Unsupported storage type: {StorageConfiguration.FILE_STORAGE_TYPE}")

    def read_data(self, configuration: DatasetConfiguration) -> pd.DataFrame:
        df: pd.DataFrame = self.storage_strategy.read_data(configuration.input_io_configuration.file_name)
        return df

    def save_data(self, df: pd.DataFrame, configuration: DatasetConfiguration) -> pd.DataFrame:
        self.storage_strategy.save_data(df, configuration.output_io_configuration.file_name,
                                        configuration.schema_configuration.file_name)
        return df
