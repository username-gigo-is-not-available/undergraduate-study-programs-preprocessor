import logging
import os
import re
from pathlib import Path

from dotenv import dotenv_values

ECTS_VALUE: int = 6
PROFESSOR_TITLES: list[str] = ["ворн.", "проф.", "д-р", "доц."]
INVALID_COURSE_CODES_REGEX: re.Pattern[str] = re.compile(r'^F23L[1-3][SW]\d{3}')

ENVIRONMENT_VARIABLES: dict[str, str] = dotenv_values(".env")

logging.info("Validating environment variables...")
for variable_name, variable_value in ENVIRONMENT_VARIABLES.items():
    if not variable_value:
        raise RuntimeError(f"{variable_name} is not set in the environment variables file!")
logging.info("Environment variables are valid!")

THREADS_PER_CPU_CORE: int = 5
MAX_WORKERS: int = THREADS_PER_CPU_CORE * os.cpu_count() if ENVIRONMENT_VARIABLES.get('MAX_WORKERS') == 'MAX_WORKERS' else (
    int(ENVIRONMENT_VARIABLES.get('MAX_WORKERS')))
OUTPUT_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH', '.'))
STUDY_PROGRAMS_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_INPUT_DATA_FILE_PATH'))
CURRICULA_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('CURRICULA_INPUT_DATA_FILE_PATH'))
COURSES_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_INPUT_DATA_FILE_PATH'))
MERGED_DATA_OUTPUT_FILE_NAME: str = ENVIRONMENT_VARIABLES.get('MERGED_DATA_OUTPUT_FILE_NAME')
COURSES_OUTPUT_FILE_NAME: str = ENVIRONMENT_VARIABLES.get('COURSES_OUTPUT_FILE_NAME')
CURRICULA_OUTPUT_FILE_NAME: str = ENVIRONMENT_VARIABLES.get('CURRICULA_OUTPUT_FILE_NAME')
STUDY_PROGRAMS_OUTPUT_FILE_NAME: str = ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_OUTPUT_FILE_NAME')
