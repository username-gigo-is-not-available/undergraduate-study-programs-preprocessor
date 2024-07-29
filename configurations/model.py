import asyncio
import logging
from pathlib import Path

import pandas as pd


class Configuration:
    def __init__(self, configuration_name: str,
                 dataset_name: str,
                 input_file_path: Path,
                 output_file_name: Path,
                 output_directory_path: Path,
                 order_by: list[str],
                 columns_order: list[str],
                 pipeline: callable):
        self.configuration_name: str = configuration_name
        self.dataset_name: str = dataset_name
        self.input_file_path: Path = input_file_path
        self.output_file_name: Path = output_file_name
        self.output_directory_path: Path = output_directory_path
        self.order_by: list[str] = order_by
        self.columns_order: list[str] = columns_order
        self.pipeline: callable = pipeline
        self.dataframe: pd.DataFrame | None = None

    async def read_csv_file(self) -> pd.DataFrame:
        logging.info(f"Reading data from file: {self.input_file_path}")
        return pd.read_csv(self.input_file_path)

    def run_read_csv_file(self) -> pd.DataFrame:
        self.dataframe = asyncio.run(self.read_csv_file())
        return self.dataframe

    async def save_csv_file(self) -> None:
        Path(self.output_directory_path).mkdir(parents=True, exist_ok=True)
        self.dataframe.to_csv(Path(self.output_directory_path, f"{self.output_file_name}.csv"), index=False)

    def run_save_csv_file(self) -> None:
        logging.info(f"Saving data to file: {self.output_file_name}.csv")
        return asyncio.run(self.save_csv_file())

    def run_pipeline(self) -> pd.DataFrame:
        logging.info(f"Running pipeline for {self.dataset_name}")
        self.dataframe = self.pipeline(self.dataframe)[self.columns_order]
        self.dataframe = self.dataframe.sort_values(by=self.order_by, ignore_index=True)
        return self.dataframe
