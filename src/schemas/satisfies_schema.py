from pyiceberg.schema import Schema
from pyiceberg.types import (
    UUIDType,
)
from pyiceberg.schema import NestedField

SATISFIES_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="satisfies_id",
        field_type=UUIDType(),
        required=True,
        doc="Universally unique identifier (UUID) of the relationship linking the course to its requisite.",
    ),
    NestedField(
        field_id=2,
        name="prerequisite_course_id",
        field_type=UUIDType(),
        required=True,
        doc="Identifier of the course that satisfies the requisite.",
    ),
    NestedField(
        field_id=3,
        name="requisite_id",
        field_type=UUIDType(),
        required=True,
        doc="Identifier of the requisite that must be satisfied by the course.",
    ),
)