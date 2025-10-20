from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
)
from pyiceberg.schema import NestedField

PROFESSOR_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="professor_id",
        field_type=StringType(),
        required=True,
        doc="Universally unique identifier (UUID) of the professor",
    ),
    NestedField(
        field_id=2,
        name="professor_name",
        field_type=StringType(),
        required=True,
        doc="The name of the professor",
    ),
    NestedField(
        field_id=3,
        name="professor_surname",
        field_type=StringType(),
        required=True,
        doc="The surname of the professor",
    ),
)