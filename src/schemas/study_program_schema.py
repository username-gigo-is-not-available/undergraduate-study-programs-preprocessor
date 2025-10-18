from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    UUIDType,
    IntegerType,
)
from pyiceberg.schema import NestedField

STUDY_PROGRAM_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="study_program_id",
        field_type=UUIDType(),
        required=True,
        doc="Universally unique identifier (UUID) of the study program",
    ),
    NestedField(
        field_id=2,
        name="study_program_code",
        field_type=StringType(),
        required=True,
        doc="The unique identifier code for the study program (pattern: ^[A-Z]{2,3}\\d{1}).",
    ),
    NestedField(
        field_id=3,
        name="study_program_name",
        field_type=StringType(),
        required=True,
        doc="The official name of the study program.",
    ),
    NestedField(
        field_id=4,
        name="study_program_duration",
        field_type=IntegerType(),
        required=True,
        doc="The standard duration of the study program in academic years (e.g. 2, 3, 4)",
    ),
    NestedField(
        field_id=5,
        name="study_program_url",
        field_type=StringType(),
        required=True,
        doc="The unique URL to the official study program description or page.",
    ),
)