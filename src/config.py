import os
import re
from pathlib import Path

from dotenv import dotenv_values


class Config:
    ECTS_VALUE: int = 6
    PROFESSOR_TITLES: list[str] = ["ворн. ", "проф. ", "д-р ", "доц. "]
    STUDY_PROGRAM_CODE_REGEX: re.Pattern[str] = re.compile(r'^(.*?)(?=23)')
    MAXIMUM_SIMILARITY_RATIO: int = 1

    ENVIRONMENT_VARIABLES: dict[str, str] = {**dotenv_values('../.env'), **os.environ}

    FILE_STORAGE_TYPE: str = ENVIRONMENT_VARIABLES.get('FILE_STORAGE_TYPE')
    MINIO_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get('MINIO_ENDPOINT_URL')
    MINIO_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get('MINIO_SECRET_KEY')
    MINIO_SOURCE_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('MINIO_SOURCE_BUCKET_NAME')
    MINIO_DESTINATION_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('MINIO_DESTINATION_BUCKET_NAME')
    # MINIO_SECURE_CONNECTION: bool = bool(ENVIRONMENT_VARIABLES.get('MINIO_SECURE_CONNECTION'))

    INPUT_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get('INPUT_DIRECTORY_PATH', '..'))
    STUDY_PROGRAMS_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_INPUT_DATA_FILE_PATH'))
    STUDY_PROGRAMS_INPUT_COLUMNS: list[str] = [
        'study_program_name',
        'study_program_duration',
        'study_program_url'
    ]
    CURRICULA_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('CURRICULA_INPUT_DATA_FILE_PATH'))
    CURRICULA_INPUT_COLUMNS: list[str] = [
        'study_program_name',
        'study_program_duration',
        'course_code',
        'course_name_mk',
        'course_type',
        'course_semester'
    ]
    COURSES_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_INPUT_DATA_FILE_PATH'))
    COURSES_INPUT_COLUMNS: list[str] = [
        'course_code',
        'course_name_mk',
        'course_name_en',
        'course_url',
    ]
    PROFESSORS_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_INPUT_DATA_FILE_PATH'))
    PROFESSORS_INPUT_COLUMNS: list[str] = [
        'course_professors',
        'course_code'
    ]
    REQUISITES_INPUT_DATA_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_INPUT_DATA_FILE_PATH'))
    REQUISITES_INPUT_COLUMNS: list[str] = [
        'course_code',
        'course_name_mk',
        'course_prerequisites'
    ]

    OUTPUT_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('OUTPUT_DIRECTORY_PATH', '..'))
    STUDY_PROGRAMS_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME'))
    STUDY_PROGRAMS_OUTPUT_COLUMNS: list[str] = [
        'study_program_id',
        'study_program_code',
        'study_program_name',
        'study_program_duration',
        'study_program_url'
    ]
    COURSES_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('COURSES_DATA_OUTPUT_FILE_NAME'))
    COURSES_OUTPUT_COLUMNS: list[str] = [
        'course_id',
        'course_code',
        'course_name_mk',
        'course_name_en',
        'course_url',
        'course_level'
    ]
    CURRICULA_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('CURRICULA_DATA_OUTPUT_FILE_NAME'))
    CURRICULA_OUTPUT_COLUMNS: list[str] = [
        'curriculum_id',
        'course_type',
        'course_semester_season',
        'course_academic_year',
        'course_semester'
    ]
    PROFESSORS_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('PROFESSORS_DATA_OUTPUT_FILE_NAME'))
    PROFESSORS_OUTPUT_COLUMNS: list[str] = [
        'professor_id',
        'professor_name',
        'professor_surname'
    ]
    TEACHES_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('TEACHES_DATA_OUTPUT_FILE_NAME'))
    TEACHES_OUTPUT_COLUMNS: list[str] = [
        'teaches_id',
        'course_id',
        'professor_id'
    ]
    INCLUDES_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('INCLUDES_DATA_OUTPUT_FILE_NAME'))
    INCLUDES_OUTPUT_COLUMNS: list[str] = [
        'includes_id',
        'curriculum_id',
        'course_id',
    ]
    OFFERS_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('OFFERS_DATA_OUTPUT_FILE_NAME'))
    OFFERS_OUTPUT_COLUMNS: list[str] = [
        'offers_id',
        'curriculum_id',
        'study_program_id',
    ]
    REQUISITES_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('REQUISITES_DATA_OUTPUT_FILE_NAME'))
    REQUISITES_OUTPUT_COLUMN_ORDER: list[str] = [
        'requisite_id',
        'course_prerequisite_type',
        'minimum_required_number_of_courses',
    ]
    PREREQUISITES_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('PREREQUISITES_OUTPUT_FILE_NAME'))
    PREREQUISITES_OUTPUT_COLUMN_ORDER: list[str] = [
        'prerequisite_id',
        'course_prerequisite_id',
        'requisite_id',
    ]
    POSTREQUISITES_OUTPUT_FILE_NAME: Path = Path(ENVIRONMENT_VARIABLES.get('POSTREQUISITES_OUTPUT_FILE_NAME'))
    POSTREQUISITES_OUTPUT_COLUMN_ORDER: list[str] = [
        'postrequisite_id',
        'course_id',
        'requisite_id',
    ]
