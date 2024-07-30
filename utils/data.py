import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

from configurations.model import Configuration
from static import ENVIRONMENT_VARIABLES, OUTPUT_DIRECTORY_PATH


async def run_process_data(executor: ThreadPoolExecutor, config: Configuration) -> Configuration:
    loop = asyncio.get_event_loop()

    await loop.run_in_executor(executor, config.run_read_csv_file)

    await loop.run_in_executor(executor, config.run_pipeline)

    await loop.run_in_executor(executor, config.run_save_csv_file)

    return config


def drop_and_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns_to_drop = [col for col in df.columns if '_x' in col]
    columns_to_rename = {col: col.replace('_y', '') for col in df.columns if '_y' in col}

    return df.drop(columns=columns_to_drop).rename(columns=columns_to_rename)


def merge_dataframes(df_curricula: pd.DataFrame, df_courses: pd.DataFrame, df_study_programs: pd.DataFrame) -> pd.DataFrame:
    logging.info("Joining dataframes df_study_programs and df_curricula on study_program_name and study_program_duration")
    merged_df = pd.merge(df_study_programs, df_curricula, how='inner', on=['study_program_name', 'study_program_duration'])
    merged_df = drop_and_rename_columns(merged_df)

    logging.info("Joining dataframes merged_df and df_courses on course_code")
    merged_df = pd.merge(merged_df, df_courses, how='inner', on='course_code')
    merged_df = drop_and_rename_columns(merged_df)

    return merged_df


def get_dataset_name_from_configs(configs: list[Configuration], dataset_name: str) -> Configuration:
    return next(filter(lambda x: x.dataset_name == dataset_name, configs))


async def run_join_data(executor: ThreadPoolExecutor, configs: list[Configuration]) -> None:
    loop = asyncio.get_event_loop()

    study_programs: Configuration = get_dataset_name_from_configs(configs, 'study_programs')
    curricula: Configuration = get_dataset_name_from_configs(configs, 'curricula')
    courses: Configuration = get_dataset_name_from_configs(configs, 'courses')

    df_merged = await loop.run_in_executor(executor, merge_dataframes, curricula.dataframe, courses.dataframe,
                                           study_programs.dataframe)

    df_merged.sort_values(by=['study_program_id', 'course_id'], ignore_index=True, inplace=True)
    logging.info(f"Saving merged data to file: {ENVIRONMENT_VARIABLES['MERGED_DATA_OUTPUT_FILE_NAME']}.csv")
    loop.run_in_executor(executor, df_merged.to_csv,
                         Path(OUTPUT_DIRECTORY_PATH, f"{ENVIRONMENT_VARIABLES['MERGED_DATA_OUTPUT_FILE_NAME']}.csv"))
