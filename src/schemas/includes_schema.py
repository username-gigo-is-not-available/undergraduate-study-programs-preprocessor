from pyiceberg.schema import Schema
from pyiceberg.types import (
     StringType,
)
from pyiceberg.schema import NestedField

INCLUDES_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="includes_id",
        field_type=StringType(),
        required=True,
        doc="Universally unique identifier (UUID) of the include relationship between the curriculum and the course.",
    ),
    NestedField(
        field_id=2,
        name="curriculum_id",
        field_type=StringType(),
        required=True,
        doc="Identifier of the curriculum in which the course is included.",
    ),
    NestedField(
        field_id=3,
        name="course_id",
        field_type=StringType(),
        required=True,
        doc="Identifier of the course that is included by the curriculum.",
    ),
)