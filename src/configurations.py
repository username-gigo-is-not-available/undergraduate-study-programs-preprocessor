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
    MINIO_INPUT_DATA_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_INPUT_DATA_BUCKET_NAME")
    MINIO_OUTPUT_DATA_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_OUTPUT_DATA_BUCKET_NAME")
    MINIO_SCHEMA_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get("MINIO_SCHEMA_BUCKET_NAME")

    INPUT_DATA_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get("INPUT_DATA_DIRECTORY_PATH", ".."))
    OUTPUT_DATA_DIRECTORY_PATH = Path(ENVIRONMENT_VARIABLES.get("OUTPUT_DATA_DIRECTORY_PATH", ".."))
    SCHEMA_DIRECTORY_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('SCHEMA_DIRECTORY_PATH', '..'))


class PathConfiguration:
    STUDY_PROGRAMS_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_DATA_INPUT_FILE_NAME"))
    STUDY_PROGRAMS_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME"))

    CURRICULA_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("CURRICULA_DATA_INPUT_FILE_NAME"))
    CURRICULA_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("CURRICULA_DATA_OUTPUT_FILE_NAME"))

    COURSES_INPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("COURSES_DATA_INPUT_FILE_NAME"))
    COURSES_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("COURSES_DATA_OUTPUT_FILE_NAME"))

    REQUISITES_INPUT_DATA: Path = COURSES_INPUT_DATA
    REQUISITES_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("REQUISITES_DATA_OUTPUT_FILE_NAME"))

    PROFESSORS_INPUT_DATA: Path = COURSES_INPUT_DATA
    PROFESSORS_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("PROFESSORS_DATA_OUTPUT_FILE_NAME"))

    OFFERS_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("OFFERS_DATA_OUTPUT_FILE_NAME"))
    INCLUDES_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("INCLUDES_DATA_OUTPUT_FILE_NAME"))
    REQUIRES_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("REQUIRES_DATA_OUTPUT_FILE_NAME"))
    SATISFIES_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("SATISFIES_DATA_OUTPUT_FILE_NAME"))
    TEACHES_OUTPUT_DATA: Path = Path(ENVIRONMENT_VARIABLES.get("TEACHES_DATA_OUTPUT_FILE_NAME"))

    STUDY_PROGRAMS_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("STUDY_PROGRAMS_SCHEMA_FILE_NAME"))
    CURRICULA_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("CURRICULA_SCHEMA_FILE_NAME"))
    COURSES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("COURSES_SCHEMA_FILE_NAME"))
    REQUISITES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("REQUISITES_SCHEMA_FILE_NAME"))
    PROFESSORS_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("PROFESSORS_SCHEMA_FILE_NAME"))
    OFFERS_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("OFFERS_SCHEMA_FILE_NAME"))
    INCLUDES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("INCLUDES_SCHEMA_FILE_NAME"))
    REQUIRES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("REQUIRES_SCHEMA_FILE_NAME"))
    SATISFIES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("SATISFIES_SCHEMA_FILE_NAME"))
    TEACHES_SCHEMA: Path = Path(ENVIRONMENT_VARIABLES.get("TEACHES_SCHEMA_FILE_NAME"))


class DatasetIOConfiguration:
    def __init__(self, file_name: str | Path,
                 columns: list[str] | None = None,
                 drop_duplicates: bool = True,
                 drop_na: bool = False):
        self.file_name = file_name
        self.columns = columns
        self.drop_duplicates = drop_duplicates
        self.drop_na = drop_na


class DatasetConfiguration:
    STUDY_PROGRAMS: "DatasetConfiguration"
    CURRICULA: "DatasetConfiguration"
    COURSES: "DatasetConfiguration"
    REQUISITES: "DatasetConfiguration"
    PROFESSORS: "DatasetConfiguration"
    OFFERS: "DatasetConfiguration"
    INCLUDES: "DatasetConfiguration"
    REQUIRES: "DatasetConfiguration"
    SATISFIES: "DatasetConfiguration"
    TEACHES: "DatasetConfiguration"

    def __init__(self,
                 dataset: DatasetType,
                 input_io_configuration: DatasetIOConfiguration | None,
                 output_io_configuration: DatasetIOConfiguration,
                 schema_configuration: DatasetIOConfiguration,
                 ):
        self.dataset_name = dataset
        self.input_io_configuration = input_io_configuration
        self.output_io_configuration = output_io_configuration
        self.schema_configuration = schema_configuration


DatasetConfiguration.STUDY_PROGRAMS = DatasetConfiguration(
    dataset=DatasetType.STUDY_PROGRAMS,
    input_io_configuration=DatasetIOConfiguration(file_name=PathConfiguration.STUDY_PROGRAMS_INPUT_DATA,
                                                  columns=
                                                  [
                                                      "study_program_name",
                                                      "study_program_duration",
                                                      "study_program_url"
                                                  ]
                                                  ),
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.STUDY_PROGRAMS_OUTPUT_DATA,
                                                   columns=[
                                                       "study_program_id",
                                                       "study_program_code",
                                                       "study_program_name",
                                                       "study_program_duration",
                                                       "study_program_url"
                                                   ],
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.STUDY_PROGRAMS_SCHEMA),
)

DatasetConfiguration.CURRICULA = DatasetConfiguration(
    dataset=DatasetType.CURRICULA,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.CURRICULA_INPUT_DATA,
                                                  columns=
                                                  [
                                                      "study_program_name",
                                                      "study_program_duration",
                                                      "course_code",
                                                      "course_name_mk",
                                                      "course_type",
                                                      "course_semester"
                                                  ]
                                                  ),
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.CURRICULA_OUTPUT_DATA,
                                                   columns=
                                                   [
                                                       "curriculum_id",
                                                       "course_type",
                                                       "course_semester_season",
                                                       "course_academic_year",
                                                       "course_semester"
                                                   ]
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.CURRICULA_SCHEMA),
)

DatasetConfiguration.COURSES = DatasetConfiguration(
    dataset=DatasetType.COURSES,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.COURSES_INPUT_DATA,
                                                  columns=
                                                  [
                                                      "course_code",
                                                      "course_name_mk",
                                                      "course_name_en",
                                                      "course_url"
                                                  ]
                                                  ),
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.COURSES_OUTPUT_DATA,
                                                   columns=
                                                   [
                                                       "course_id",
                                                       "course_code",
                                                       "course_name_mk",
                                                       "course_name_en",
                                                       "course_url",
                                                       "course_level"
                                                   ]
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.COURSES_SCHEMA),
)

DatasetConfiguration.REQUISITES = DatasetConfiguration(
    dataset=DatasetType.REQUISITES,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.REQUISITES_INPUT_DATA,
                                                  columns=
                                                  [
                                                      "course_prerequisites",
                                                      "course_code"
                                                  ]
                                                  ),
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.REQUISITES_OUTPUT_DATA,
                                                   columns=
                                                   [
                                                       "requisite_id",
                                                       "course_prerequisite_type",
                                                       "minimum_required_number_of_courses"
                                                   ],
                                                   drop_na=True
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.REQUISITES_SCHEMA),
)

DatasetConfiguration.PROFESSORS = DatasetConfiguration(
    dataset=DatasetType.PROFESSORS,
    input_io_configuration=DatasetIOConfiguration(PathConfiguration.PROFESSORS_INPUT_DATA,
                                                  columns=[
                                                      "course_professors",
                                                      "course_code"
                                                  ],
                                                  ),
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.PROFESSORS_OUTPUT_DATA,
                                                   columns=
                                                   [
                                                       "professor_id",
                                                       "professor_name",
                                                       "professor_surname"
                                                   ],
                                                   drop_na=True
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.PROFESSORS_SCHEMA),
)

DatasetConfiguration.OFFERS = DatasetConfiguration(
    dataset=DatasetType.OFFERS,
    input_io_configuration=None,
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.OFFERS_OUTPUT_DATA,
                                                   columns=
                                                   [
                                                       "offers_id",
                                                       "curriculum_id",
                                                       "study_program_id"
                                                   ]
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.OFFERS_SCHEMA),
)

DatasetConfiguration.INCLUDES = DatasetConfiguration(
    dataset=DatasetType.INCLUDES,
    input_io_configuration=None,
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.INCLUDES_OUTPUT_DATA,
                                                   columns=
                                                   [
                                                       "includes_id",
                                                       "curriculum_id",
                                                       "course_id"
                                                   ]
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.INCLUDES_SCHEMA),
)

DatasetConfiguration.REQUIRES = DatasetConfiguration(
    dataset=DatasetType.REQUIRES,
    input_io_configuration=None,
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.REQUIRES_OUTPUT_DATA,
                                                   columns=
                                                   [
                                                       "requires_id",
                                                       "course_id",
                                                       "requisite_id"
                                                   ],
                                                   drop_na=True
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.REQUIRES_SCHEMA),
)

DatasetConfiguration.SATISFIES = DatasetConfiguration(
    dataset=DatasetType.SATISFIES,
    input_io_configuration=None,
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.SATISFIES_OUTPUT_DATA,
                                                   columns=
                                                   [
                                                       "satisfies_id",
                                                       "prerequisite_course_id",
                                                       "requisite_id"
                                                   ],
                                                   drop_na=True
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.SATISFIES_SCHEMA),
)

DatasetConfiguration.TEACHES = DatasetConfiguration(
    dataset=DatasetType.TEACHES,
    input_io_configuration=None,
    output_io_configuration=DatasetIOConfiguration(PathConfiguration.TEACHES_OUTPUT_DATA,
                                                   columns=
                                                   [
                                                       "teaches_id",
                                                       "course_id",
                                                       "professor_id"
                                                   ],
                                                   drop_na=True
                                                   ),
    schema_configuration=DatasetIOConfiguration(PathConfiguration.TEACHES_SCHEMA),
)
