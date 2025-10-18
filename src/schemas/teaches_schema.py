from pyiceberg.schema import Schema
from pyiceberg.types import (
    UUIDType,
)
from pyiceberg.schema import NestedField

TEACHES_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="teaches_id",
        field_type=UUIDType(),
        required=True,
        doc="Universally unique identifier (UUID) of the offer linking the study program and curriculum.",
    ),
    NestedField(
        field_id=2,
        name="professor_id",
        field_type=UUIDType(),
        required=True,
        doc="Identifier of the professor teaching the course.",
    ),
    NestedField(
        field_id=3,
        name="course_id",
        field_type=UUIDType(),
        required=True,
        doc="Identifier of the course taught by the professor.",
    ),
)