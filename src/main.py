import argparse
import asyncio
import logging
import time

import pandas as pd

from src.config import Config
from src.pipeline.models.enums import DatasetType
from src.pipeline.courses_pipeline import build_courses_pipeline
from src.pipeline.curricula_pipeline import build_curricula_pipeline
from src.pipeline.merge_data_pipeline import build_merge_data_pipeline
from src.pipeline.study_program_pipeline import build_study_programs_pipeline

logging.basicConfig(level=logging.INFO)


async def main(datasets: list[DatasetType]) -> None:
    logging.info("Starting...")
    start: float = time.perf_counter()

    logging.info(f"Running pipelines: {datasets}")

    df_study_programs: pd.DataFrame | None = None
    df_curricula: pd.DataFrame | None = None
    df_courses: pd.DataFrame | None = None

    if DatasetType.STUDY_PROGRAMS in datasets:
        df_study_programs = build_study_programs_pipeline().run()
    if DatasetType.CURRICULA in datasets:
        df_curricula = build_curricula_pipeline().run()
    if DatasetType.COURSES in datasets:
        df_courses = build_courses_pipeline().run()
    if DatasetType.MERGED_DATA in datasets:
        build_merge_data_pipeline(df_study_programs=df_study_programs, df_curricula=df_curricula, df_courses=df_courses).run()

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description='Run ETL pipeline')
    parser.add_argument('--datasets', type=str, nargs='+',
                        help=f'List of datasets to run, valid datasets are: {list(DatasetType)}',
                        default=list(DatasetType))

    args = parser.parse_args()
    datasets: list[DatasetType] = args.datasets

    asyncio.run(main(datasets))
