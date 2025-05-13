import os
import re
from pathlib import Path

from dotenv import dotenv_values


class Config:
    ECTS_VALUE: int = 6
    PROFESSOR_TITLES: list[str] = ["ворн.", "проф.", "д-р", "доц."]
    STUDY_PROGRAM_CODE_REGEX: re.Pattern[str] = re.compile(r'^(.*?)(?=23)')
    COURSE_CODES_REGEX: re.Pattern[str] = re.compile(r'^F23L[1-3][SW]\d{3}')
    COURSE_SIMILARITY_THRESHOLD: float = 0.835

    ENVIRONMENT_VARIABLES: dict[str, str] = {**dotenv_values('../.env'), **os.environ}

    FILE_STORAGE_TYPE: str = ENVIRONMENT_VARIABLES.get('FILE_STORAGE_TYPE')
    MAX_WORKERS: int = os.cpu_count() if ENVIRONMENT_VARIABLES.get(
        'MAX_WORKERS') == 'MAX_WORKERS' else ENVIRONMENT_VARIABLES.get('MAX_WORKERS')
    MINIO_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get('MINIO_ENDPOINT_URL')
    MINIO_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get('MINIO_SECRET_KEY')
    MINIO_SOURCE_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('MINIO_SOURCE_BUCKET_NAME')
    MINIO_DESTINATION_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('MINIO_DESTINATION_BUCKET_NAME')
    # MINIO_SECURE_CONNECTION: bool = bool(ENVIRONMENT_VARIABLES.get('MINIO_SECURE_CONNECTION'))

    INPUT_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get('INPUT_DIRECTORY_PATH', '..'))
    STUDY_PROGRAMS_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_INPUT_DATA_FILE_PATH'))
    CURRICULA_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('CURRICULA_INPUT_DATA_FILE_PATH'))
    COURSES_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_INPUT_DATA_FILE_PATH'))
    OUTPUT_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH', '..'))
    STUDY_PROGRAMS_INPUT_COLUMN_ORDER: list[str] = [
        'study_program_name',
        'study_program_duration',
        'study_program_url'
    ]
    STUDY_PROGRAMS_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME'))
    STUDY_PROGRAMS_OUTPUT_COLUMN_ORDER: list[str] = [
        'study_program_id',
        'study_program_code',
        'study_program_name',
        'study_program_duration',
        'study_program_url'
    ]
    COURSES_INPUT_COLUMN_ORDER: list[str] = [
        'course_code',
        'course_name_mk',
        'course_name_en',
        'course_url',
    ]
    PROFESSORS_INPUT_COLUMN_ORDER: list[str] = [
        'course_professors'
    ]
    TEACHES_INPUT_COLUMN_ORDER: list[str] = [
        'course_code',
        'course_professors'
    ]
    PROFESSOR_TEACHES_COLUMN_ORDER: list[str] = [
        'course_code',
        'course_professors'
    ]
    COURSES_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_DATA_OUTPUT_FILE_NAME'))
    COURSES_OUTPUT_COLUMN_ORDER: list[str] = [
        'course_id',
        'course_code',
        'course_name_mk',
        'course_name_en',
        'course_url',
    ]
    PROFESSORS_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('PROFESSORS_DATA_OUTPUT_FILE_NAME'))
    PROFESSORS_OUTPUT_COLUMN_ORDER: list[str] = [
        'professor_id',
        'professor_name',
        'professor_surname'
    ]
    TEACHES_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('TEACHES_DATA_OUTPUT_FILE_NAME'))
    TEACHES_OUTPUT_COLUMN_ORDER: list[str] = [
        'teaches_id',
        'course_id',
        'professor_id'
    ]
    STUDY_PROGRAM_COURSE_INPUT_COLUMN_ORDER: list[str] = [
        'study_program_name',
        'study_program_duration',
        'course_code',
        'course_name_mk',
        'course_type',
        'course_semester',
        'course_prerequisites',
    ]
    OFFERS_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('OFFERS_DATA_OUTPUT_FILE_NAME'))
    OFFERS_OUTPUT_COLUMN_ORDER: list[str] = [
        'offers_id',
        'study_program_id',
        'course_id',
        'course_type',
        'course_semester',
        'course_semester_season',
        'course_academic_year',
        'course_level',
    ]
    REQUIRES_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('REQUIRES_DATA_OUTPUT_FILE_NAME'))
    REQUIRES_OUTPUT_COLUMN_ORDER: list[str] = [
        'requires_id',
        'course_id',
        'course_prerequisite_type',
        'course_prerequisite_id',
        'minimum_required_number_of_courses',
    ]
