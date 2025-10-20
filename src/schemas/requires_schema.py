from pyiceberg.schema import Schema
from pyiceberg.types import (
     StringType,
)
from pyiceberg.schema import NestedField

REQUIRES_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="requires_id",
        field_type=StringType(),
        required=True,
        doc="Universally unique identifier (UUID) of the relationship linking the course to its requisite.",
    ),
    NestedField(
        field_id=2,
        name="course_id",
        field_type=StringType(),
        required=True,
        doc="Identifier of the course that requires the requisite.",
    ),
    NestedField(
        field_id=3,
        name="requisite_id",
        field_type=StringType(),
        required=True,
        doc="Identifier of the requisite that is required by the course.",
    ),
)