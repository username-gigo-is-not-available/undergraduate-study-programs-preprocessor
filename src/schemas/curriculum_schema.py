from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    IntegerType,
)
from pyiceberg.schema import NestedField

CURRICULUM_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="curriculum_id",
        field_type=StringType(),
        required=True,
        doc="Universally unique identifier (UUID) of the curriculum",
    ),
    NestedField(
        field_id=2,
        name="course_type",
        field_type=StringType(),
        required=True,
        doc="The type of the course. (MANDATORY or ELECTIVE)",
    ),
    NestedField(
        field_id=3,
        name="course_semester_season",
        field_type=StringType(),
        required=True,
        doc="The semester season of the semester. (WINTER or SUMMER)",
    ),
    NestedField(
        field_id=4,
        name="course_semester",
        field_type=IntegerType(),
        required=True,
        doc="The semester the course is offered in (range: [1, 8])",
    ),
    NestedField(
        field_id=5,
        name="course_academic_year",
        field_type=IntegerType(),
        required=True,
        doc="The academic year the course is offered in (range: [1, 4])",
    ),
)