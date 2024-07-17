import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import logging
from pathlib import Path

import pandas as pd

from pipeline.instances.course_pipeline import run_courses_pipeline

from pipeline.instances.curricula_pipeline import run_curricula_pipeline
from pipeline.instances.study_programs_pipeline import run_study_programs_pipeline
from settings import ENVIRONMENT_VARIABLES
from static import MAX_WORKERS
from utils.files import read_data, get_input_data_file_paths, prepare_data_for_saving, save_data

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    logging.info("Starting...")
    start: float = time.perf_counter()
    output_directory: str = ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH', '.')
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as thread_executor:
        input_data_file_paths: dict[str, Path] = get_input_data_file_paths()
        raw_data: dict[str, pd.DataFrame] = await read_data(thread_executor, input_data_file_paths)

        run_courses_pipeline(raw_data.get('courses'))
        run_curricula_pipeline(raw_data.get('curricula'))
        run_study_programs_pipeline(raw_data.get('study_programs'))

        processed_data: list[dict[str, str | pd.DataFrame]] = prepare_data_for_saving(raw_data.get('study_programs'),
                                                                                      raw_data.get('curricula'),
                                                                                      raw_data.get('courses'))

        await save_data(thread_executor, output_directory, processed_data)

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
    '''
    think about adding surrogate ids for every model
    '''