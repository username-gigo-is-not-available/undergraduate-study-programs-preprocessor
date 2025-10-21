import logging
import pyarrow as pa

import pandas as pd
from minio import Minio
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.table import Table
from src.configurations import StorageConfiguration, DatasetConfiguration
from src.pipeline.models.enums import FileIOType


class IcebergClient:
    _s3_client: Minio = None
    _catalog: Catalog = None

    def __init__(self):
        if self._catalog is not None:
            return
        logging.info("Initializing IcebergClient resources...")
        self._catalog = load_catalog(
            StorageConfiguration.ICEBERG_CATALOG_NAME
        )
        if self._s3_client is None and StorageConfiguration.FILE_IO_TYPE == FileIOType.S3:
            self._s3_client = Minio(
                endpoint=StorageConfiguration.S3_ENDPOINT_URL,
                access_key=StorageConfiguration.S3_ACCESS_KEY,
                secret_key=StorageConfiguration.S3_SECRET_KEY,
                secure=False
            )

    def get_catalog(self) -> Catalog:
        return self._catalog

    def get_s3_client(self) -> Minio | None:
        return self._s3_client

    @classmethod
    def generate_table_identifier(cls, namespace: str, table_name: str) -> str:
        return f"{namespace}.{table_name}"

    def get_table(self, namespace: str, table_name: str) -> Table:
        catalog: Catalog = self.get_catalog()
        table_identifier: str = self.generate_table_identifier(namespace, table_name)
        logging.info(f"Loading table {table_identifier}")
        return catalog.load_table(table_identifier)

    def read_data(self, dataset_configuration: DatasetConfiguration) -> pd.DataFrame:
        table: Table = self.get_table(StorageConfiguration.ICEBERG_SOURCE_NAMESPACE, dataset_configuration.input_table_configuration.table_name)
        return table.scan(selected_fields=tuple(dataset_configuration.input_table_configuration.columns)).to_pandas()

    def save_data(self, df: pd.DataFrame, dataset_configuration: DatasetConfiguration) -> pd.DataFrame:
        table: Table = self.get_table(StorageConfiguration.ICEBERG_DESTINATION_NAMESPACE,
                                      dataset_configuration.output_table_configuration.table_name)

        df_to_append: pd.DataFrame = (
            df.copy(deep=True)
            [dataset_configuration.output_table_configuration.columns]
            .drop_duplicates()
            .dropna()
        )

        logging.info(f"Saving data to {table.name()} with schema {table.schema()} and {len(df_to_append)} rows")

        table.append((pa.Table.from_pandas(df=df_to_append, schema=dataset_configuration.schema.as_arrow())
                      .select(dataset_configuration.output_table_configuration.columns)))

        logging.info(f"Created snapshot_id: {table.current_snapshot().snapshot_id} for table {table.name()}")
        return df
