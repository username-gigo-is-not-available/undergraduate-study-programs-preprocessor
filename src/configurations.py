import os
import re
from pathlib import Path

from dotenv import dotenv_values

from src.pipeline.models.enums import DatasetType

ENVIRONMENT_VARIABLES: dict[str, str] = {**dotenv_values('../.env'), **os.environ}


class ApplicationConfiguration:
    ECTS_VALUE: int = 6
    PROFESSOR_TITLES: list[str] = ["ворн. ", "проф. ", "д-р ", "доц. "]
    STUDY_PROGRAM_CODE_REGEX: re.Pattern[str] = re.compile(r'^(.*?)(?=23)')
    MAXIMUM_SIMILARITY_RATIO: int = 1


class StorageConfiguration:
    FILE_STORAGE_TYPE: str = ENVIRONMENT_VARIABLES.get("FILE_STORAGE_TYPE")
    MINIO_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get("MINIO_ENDPOINT_URL")
    MINIO_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get("MINIO_SECRET_KEY")
    MINIO_SOURCE_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_SOURCE_BUCKET_NAME")
    MINIO_DESTINATION_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_DESTINATION_BUCKET_NAME")

    INPUT_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get("INPUT_DIRECTORY_PATH", ".."))
    OUTPUT_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get("OUTPUT_DIRECTORY_PATH", ".."))

    INPUT_FILE_LOCATION: str | Path = (
        INPUT_DIRECTORY_PATH if FILE_STORAGE_TYPE == "LOCAL" else MINIO_SOURCE_BUCKET_NAME
    )
    OUTPUT_FILE_LOCATION: str | Path = (
        OUTPUT_DIRECTORY_PATH if FILE_STORAGE_TYPE == "LOCAL" else MINIO_DESTINATION_BUCKET_NAME
    )


class DatasetIOConfiguration:
    def __init__(self, file_name: str | Path):
        self.file_name = file_name


class DatasetPathConfiguration:
    STUDY_PROGRAMS_INPUT = Path(ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_DATA_INPUT_FILE_NAME"))
    STUDY_PROGRAMS_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME"))

    COURSES_INPUT = Path(ENVIRONMENT_VARIABLES.get("COURSES_DATA_INPUT_FILE_NAME"))
    COURSES_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("COURSES_DATA_OUTPUT_FILE_NAME"))

    PROFESSORS_INPUT = COURSES_INPUT
    PROFESSORS_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("PROFESSORS_DATA_OUTPUT_FILE_NAME"))

    CURRICULA_INPUT = Path(ENVIRONMENT_VARIABLES.get("CURRICULA_DATA_INPUT_FILE_NAME"))
    CURRICULA_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("CURRICULA_DATA_OUTPUT_FILE_NAME"))

    REQUISITES_INPUT = COURSES_INPUT
    REQUISITES_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("REQUISITES_DATA_OUTPUT_FILE_NAME"))

    OFFERS_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("OFFERS_DATA_OUTPUT_FILE_NAME"))
    INCLUDES_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("INCLUDES_DATA_OUTPUT_FILE_NAME"))
    PREREQUISITES_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("PREREQUISITES_DATA_OUTPUT_FILE_NAME"))
    POSTREQUISITES_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("POSTREQUISITES_DATA_OUTPUT_FILE_NAME"))
    TEACHES_OUTPUT = Path(ENVIRONMENT_VARIABLES.get("TEACHES_DATA_OUTPUT_FILE_NAME"))


class DatasetTransformationConfiguration:
    def __init__(self, columns: list[str], drop_duplicates: bool = False, drop_na: bool = False):
        self.columns = columns
        self.drop_duplicates = drop_duplicates
        self.drop_na = drop_na


class DatasetConfiguration:
    STUDY_PROGRAMS: "DatasetConfiguration"
    COURSES: "DatasetConfiguration"
    PROFESSORS: "DatasetConfiguration"
    CURRICULA: "DatasetConfiguration"
    REQUISITES: "DatasetConfiguration"
    OFFERS: "DatasetConfiguration"
    INCLUDES: "DatasetConfiguration"
    PREREQUISITES: "DatasetConfiguration"
    POSTREQUISITES: "DatasetConfiguration"
    TEACHES: "DatasetConfiguration"


    def __init__(self,
                 dataset: DatasetType,
                 input_io_config: DatasetIOConfiguration | None,
                 output_io_config: DatasetIOConfiguration,
                 input_transformation_config: DatasetTransformationConfiguration | None,
                 output_transformation_config: DatasetTransformationConfiguration,
                 ):
        self.dataset_name = dataset
        self.input_io_config = input_io_config
        self.output_io_config = output_io_config
        self.input_transformation_config = input_transformation_config
        self.output_transformation_config = output_transformation_config


DatasetConfiguration.STUDY_PROGRAMS = DatasetConfiguration(
    dataset=DatasetType.STUDY_PROGRAMS,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.STUDY_PROGRAMS_INPUT),
    input_transformation_config=DatasetTransformationConfiguration(columns=[
        "study_program_name", "study_program_duration", "study_program_url"
    ]),
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.STUDY_PROGRAMS_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "study_program_id", "study_program_code", "study_program_name",
        "study_program_duration", "study_program_url"
    ])
)

DatasetConfiguration.COURSES = DatasetConfiguration(
    dataset=DatasetType.COURSES,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.COURSES_INPUT),
    input_transformation_config=DatasetTransformationConfiguration(columns=[
        "course_code", "course_name_mk", "course_name_en", "course_url"
    ]),
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.COURSES_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "course_id", "course_code", "course_name_mk", "course_name_en",
        "course_url", "course_level"
    ])
)

DatasetConfiguration.PROFESSORS = DatasetConfiguration(
    dataset=DatasetType.PROFESSORS,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.PROFESSORS_INPUT),
    input_transformation_config=DatasetTransformationConfiguration(columns=[
        "course_professors", "course_code"
    ]),
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.PROFESSORS_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "professor_id", "professor_name", "professor_surname"
    ],
        drop_na=True,
    )
)

DatasetConfiguration.CURRICULA = DatasetConfiguration(
    dataset=DatasetType.CURRICULA,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.CURRICULA_INPUT),
    input_transformation_config=DatasetTransformationConfiguration(columns=[
        "study_program_name", "study_program_duration", "course_code",
        "course_name_mk", "course_type", "course_semester"
    ]),
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.CURRICULA_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "curriculum_id", "course_type", "course_semester_season",
        "course_academic_year", "course_semester"
    ])
)

DatasetConfiguration.REQUISITES = DatasetConfiguration(
    dataset=DatasetType.REQUISITES,
    input_io_config=DatasetIOConfiguration(DatasetPathConfiguration.REQUISITES_INPUT),
    input_transformation_config=DatasetTransformationConfiguration(columns=[
        "course_code", "course_name_mk", "course_prerequisites"
    ]),
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.REQUISITES_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "requisite_id", "course_prerequisite_type", "minimum_required_number_of_courses"
    ],
        drop_na=True)
)
DatasetConfiguration.OFFERS = DatasetConfiguration(
    dataset=DatasetType.OFFERS,
    input_io_config=None,
    input_transformation_config=None,
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.OFFERS_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "offers_id", "curriculum_id", "study_program_id"
    ])
)
DatasetConfiguration.INCLUDES = DatasetConfiguration(
    dataset=DatasetType.INCLUDES,
    input_io_config=None,
    input_transformation_config=None,
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.INCLUDES_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "includes_id", "curriculum_id", "study_program_id"
    ])
)
DatasetConfiguration.PREREQUISITES = DatasetConfiguration(
    dataset=DatasetType.PREREQUISITES,
    input_io_config=None,
    input_transformation_config=None,
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.PREREQUISITES_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "prerequisite_id", "course_prerequisite_id", "requisite_id"
    ],
        drop_na=True)
)
DatasetConfiguration.POSTREQUISITES = DatasetConfiguration(
    dataset=DatasetType.POSTREQUISITES,
    input_io_config=None,
    input_transformation_config=None,
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.POSTREQUISITES_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "postrequisite_id", "course_id", "requisite_id"
    ],
        drop_na=True)
)
DatasetConfiguration.TEACHES = DatasetConfiguration(
    dataset=DatasetType.TEACHES,
    input_io_config=None,
    input_transformation_config=None,
    output_io_config=DatasetIOConfiguration(DatasetPathConfiguration.TEACHES_OUTPUT),
    output_transformation_config=DatasetTransformationConfiguration(columns=[
        "teaches_id", "course_id", "professor_id"
    ],
        drop_na=True)
)
