import asyncio
import logging
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd

from settings import ENVIRONMENT_VARIABLES


def run_read_csv_file(path: str) -> pd.DataFrame:
    return asyncio.run(read_csv_file(path))


async def read_data(executor: ThreadPoolExecutor, input_data_file_paths: dict[str, str]) -> \
        dict[str, pd.DataFrame]:
    loop = asyncio.get_event_loop()
    tasks = {name: loop.run_in_executor(executor, run_read_csv_file, path) for name, path in input_data_file_paths.items()}

    return {name: await task for name, task in tasks.items()}


async def read_csv_file(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def get_input_data_file_paths() -> dict[str, str]:
    return {
        "study_programs": ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_INPUT_DATA_FILE_PATH'),
        "curricula": ENVIRONMENT_VARIABLES.get('CURRICULA_INPUT_DATA_FILE_PATH'),
        "courses": ENVIRONMENT_VARIABLES.get('COURSE_INPUT_DATA_FILE_PATH'),
    }


def prepare_data_for_saving(df_study_programs: pd.DataFrame,
                            df_curricula: pd.DataFrame,
                            df_courses: pd.DataFrame) -> list[dict[str, str | pd.DataFrame]]:
    return [
        {
            'file_name': ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME'),
            'data': df_study_programs
        },
        {
            'file_name': ENVIRONMENT_VARIABLES.get('CURRICULA_DATA_OUTPUT_FILE_NAME'),
            'data': df_curricula
        },
        {
            'file_name': ENVIRONMENT_VARIABLES.get('COURSE_DATA_OUTPUT_FILE_NAME'),
            'data': df_courses
        }
    ]


def run_save_data_to_file(data: dict[str,], output_dir: str) -> None:
    return asyncio.run(save_data_to_file(data, output_dir))


async def save_data_to_file(data: dict[str, str | pd.DataFrame], output_dir: str) -> None:
    file_name: str = data.get("file_name")
    data: pd.DataFrame = data.get("data")
    logging.info(f"Saving data to file: {file_name}.csv")
    data.to_csv(f"{output_dir}/{file_name}.csv", index=False)


async def save_data(executor: ThreadPoolExecutor, output_dir: str,
                    data: list[dict[str, str | pd.DataFrame]]) -> None:
    loop = asyncio.get_event_loop()
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    tasks: list[Future[None]] = [loop.run_in_executor(executor, run_save_data_to_file, item, output_dir) for item in data]
    await asyncio.gather(*tasks)
