import re
from pathlib import Path

from pyiceberg.schema import Schema

from src.pipeline.models.enums import FileIOType
from src.schemas.course_schema import COURSE_SCHEMA
from src.schemas.curriculum_schema import CURRICULUM_SCHEMA
from src.schemas.includes_schema import INCLUDES_SCHEMA
from src.schemas.offers_schema import OFFERS_SCHEMA
from src.schemas.professor_schema import PROFESSOR_SCHEMA
from src.schemas.requires_schema import REQUIRES_SCHEMA
from src.schemas.requisite_schema import REQUISITE_SCHEMA
from src.schemas.satisfies_schema import SATISFIES_SCHEMA
from src.schemas.study_program_schema import STUDY_PROGRAM_SCHEMA
from src.schemas.teaches_schema import TEACHES_SCHEMA
from src.setup import ENVIRONMENT_VARIABLES


class ApplicationConfiguration:
    ECTS_VALUE: int = 6
    PROFESSOR_TITLES: list[str] = ["ворн. ", "проф. ", "д-р ", "доц. "]
    STUDY_PROGRAM_CODE_REGEX: re.Pattern[str] = re.compile(r'^(.*?)(?=23)')
    MAXIMUM_SIMILARITY_RATIO: int = 1
    MINIMUM_SIMILARITY_RATIO: float = 0.75


class StorageConfiguration:
    FILE_IO_TYPE: FileIOType = FileIOType(ENVIRONMENT_VARIABLES.get('FILE_IO_TYPE').upper())
    LOCAL_ICEBERG_LAKEHOUSE_FILE_PATH: Path = Path(ENVIRONMENT_VARIABLES.get('LOCAL_ICEBERG_LAKEHOUSE_FILE_PATH'))
    S3_ENDPOINT_URL: str = ENVIRONMENT_VARIABLES.get('S3_ENDPOINT_URL')
    S3_ACCESS_KEY: str = ENVIRONMENT_VARIABLES.get('S3_ACCESS_KEY')
    S3_SECRET_KEY: str = ENVIRONMENT_VARIABLES.get('S3_SECRET_KEY')
    S3_ICEBERG_LAKEHOUSE_BUCKET_NAME: str = ENVIRONMENT_VARIABLES.get('S3_ICEBERG_LAKEHOUSE_BUCKET_NAME')
    S3_PATH_STYLE_ACCESS: bool = ENVIRONMENT_VARIABLES.get('S3_PATH_STYLE_ACCESS')
    ICEBERG_CATALOG_NAME: str = ENVIRONMENT_VARIABLES.get("ICEBERG_CATALOG_NAME")
    ICEBERG_SOURCE_NAMESPACE: str = ENVIRONMENT_VARIABLES.get("ICEBERG_SOURCE_NAMESPACE")
    ICEBERG_DESTINATION_NAMESPACE: str = ENVIRONMENT_VARIABLES.get("ICEBERG_DESTINATION_NAMESPACE")


class TableConfiguration:
    def __init__(self,
                 table_name: str,
                 columns: list[str] | None = None
                 ):
        self.table_name = table_name
        self.columns = columns


class DatasetConfiguration:
    def __init__(self,
                 dataset_name: str,
                 input_table_configuration: TableConfiguration | None,
                 output_table_configuration: TableConfiguration,
                 schema: Schema,
                 ):
        self.dataset_name = dataset_name
        self.input_table_configuration = input_table_configuration
        self.output_table_configuration = output_table_configuration
        self.schema = schema


STUDY_PROGRAMS: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATASET_NAME'),
    input_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATASET_NAME'),
                                                 columns=
                                                 [
                                                     "study_program_name",
                                                     "study_program_duration",
                                                     "study_program_url"
                                                 ]
                                                 ),
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATASET_NAME'),
                                                  columns=[
                                                      "study_program_id",
                                                      "study_program_code",
                                                      "study_program_name",
                                                      "study_program_duration",
                                                      "study_program_url"
                                                  ],
                                                  ),
    schema=STUDY_PROGRAM_SCHEMA,
)

CURRICULA: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('CURRICULA_DATASET_NAME'),
    input_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('CURRICULA_DATASET_NAME'),
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
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('CURRICULA_DATASET_NAME'),
                                                  columns=
                                                  [
                                                      "curriculum_id",
                                                      "course_type",
                                                      "course_semester_season",
                                                      "course_academic_year",
                                                      "course_semester"
                                                  ]
                                                  ),
    schema=CURRICULUM_SCHEMA,
)

COURSES: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('COURSES_DATASET_NAME'),
    input_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('COURSES_DATASET_NAME'),
                                                 columns=
                                                 [
                                                     "course_code",
                                                     "course_name_mk",
                                                     "course_name_en",
                                                     "course_url"
                                                 ]
                                                 ),
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('COURSES_DATASET_NAME'),
                                                  columns=
                                                  [
                                                      "course_id",
                                                      "course_code",
                                                      "course_name_mk",
                                                      "course_name_en",
                                                      "course_abbreviation",
                                                      "course_url",
                                                      "course_level"
                                                  ]
                                                  ),
    schema=COURSE_SCHEMA,
)

REQUISITES: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('REQUISITES_DATASET_NAME'),
    input_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('COURSES_DATASET_NAME'),
                                                 columns=
                                                 [
                                                     "course_prerequisites",
                                                     "course_name_mk"
                                                 ]
                                                 ),
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('REQUISITES_DATASET_NAME'),
                                                  columns=
                                                  [
                                                      "requisite_id",
                                                      "course_prerequisite_type",
                                                      "minimum_required_number_of_courses"
                                                  ],
                                                  ),
    schema=REQUISITE_SCHEMA,
)

PROFESSORS: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('PROFESSORS_DATASET_NAME'),
    input_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('COURSES_DATASET_NAME'),
                                                 columns=[
                                                     "course_professors",
                                                     "course_code"
                                                 ],
                                                 ),
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('PROFESSORS_DATASET_NAME'),
                                                  columns=
                                                  [
                                                      "professor_id",
                                                      "professor_name",
                                                      "professor_surname"
                                                  ],
                                                  ),
    schema=PROFESSOR_SCHEMA,
)

OFFERS: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('OFFERS_DATASET_NAME'),
    input_table_configuration=None,
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('OFFERS_DATASET_NAME'),
                                                  columns=
                                                  [
                                                      "offers_id",
                                                      "curriculum_id",
                                                      "study_program_id"
                                                  ]
                                                  ),
    schema=OFFERS_SCHEMA,
)

INCLUDES: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('INCLUDES_DATASET_NAME'),
    input_table_configuration=None,
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('INCLUDES_DATASET_NAME'),
                                                  columns=
                                                  [
                                                      "includes_id",
                                                      "curriculum_id",
                                                      "course_id"
                                                  ]
                                                  ),
    schema=INCLUDES_SCHEMA,
)

REQUIRES: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('REQUIRES_DATASET_NAME'),
    input_table_configuration=None,
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('REQUIRES_DATASET_NAME'),
                                                  columns=
                                                  [
                                                      "requires_id",
                                                      "course_id",
                                                      "requisite_id"
                                                  ],
                                                  ),
    schema=REQUIRES_SCHEMA,
)

SATISFIES: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('SATISFIES_DATASET_NAME'),
    input_table_configuration=None,
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('SATISFIES_DATASET_NAME'),
                                                  columns=
                                                  [
                                                      "satisfies_id",
                                                      "course_id",
                                                      "requisite_id"
                                                  ],
                                                  ),
    schema=SATISFIES_SCHEMA,
)

TEACHES: DatasetConfiguration = DatasetConfiguration(
    dataset_name=ENVIRONMENT_VARIABLES.get('TEACHES_DATASET_NAME'),
    input_table_configuration=None,
    output_table_configuration=TableConfiguration(table_name=ENVIRONMENT_VARIABLES.get('TEACHES_DATASET_NAME'),
                                                  columns=
                                                  [
                                                      "teaches_id",
                                                      "course_id",
                                                      "professor_id"
                                                  ],
                                                  ),
    schema=TEACHES_SCHEMA,
)
