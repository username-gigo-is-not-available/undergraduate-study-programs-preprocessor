import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import logging
from typing import Coroutine

from configurations.instance import courses_config, curricula_config, study_programs_config
from configurations.model import Configuration
from static import MAX_WORKERS
from utils.data import run_process_data, run_join_data

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    logging.info("Starting...")
    start: float = time.perf_counter()
    configs: list[Configuration] = [
        study_programs_config,
        curricula_config,
        courses_config
    ]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as thread_executor:
        tasks: list[Coroutine[Configuration]] = [run_process_data(thread_executor, config) for config in configs]
        results: list[Configuration] = [_ for _ in await asyncio.gather(*tasks)]
        await run_join_data(thread_executor, results)

    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    asyncio.run(main())
