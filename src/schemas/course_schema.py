from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    IntegerType,
)
from pyiceberg.schema import NestedField

COURSE_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="course_id",
        field_type=StringType(),
        required=True,
        doc="Universally unique identifier (UUID) of the course",
    ),
    NestedField(
        field_id=2,
        name="course_code",
        field_type=StringType(),
        required=True, 
        doc="The unique identifier code for the course (pattern: ^F23L[1-3][SW]\\d{3}).",
    ),
    NestedField(
        field_id=3,
        name="course_name_mk",
        field_type=StringType(),
        required=True, 
        doc="The official name of the course in Macedonian.",
    ),
    NestedField(
        field_id=4,
        name="course_name_en",
        field_type=StringType(),
        required=True, 
        doc="The official name of the course in English.",
    ),
    NestedField(
        field_id=5,
        name="course_abbreviation",
        field_type=StringType(),
        required=True, 
        doc="The abbreviation of the course name in Macedonian.",
    ),
    NestedField(
        field_id=6,
        name="course_url",
        field_type=StringType(),
        required=True, 
        doc="The unique URL to the official course description or page.",
    ),
    NestedField(
        field_id=7,
        name="course_level",
        field_type=IntegerType(),
        required=True,
        doc="Elective course level for the course: L1 (N1 group, max 6 ECTS), L2 (N2 group, max 36 ECTS), L3 (N3 group, unlimited ECTS)",
    ),
)

