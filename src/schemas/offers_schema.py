from pyiceberg.schema import Schema
from pyiceberg.types import (
    UUIDType,
)
from pyiceberg.schema import NestedField

OFFERS_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="offers_id",
        field_type=UUIDType(),
        required=True,
        doc="Universally unique identifier (UUID) of the offer linking the study program and curriculum.",
    ),
    NestedField(
        field_id=2,
        name="study_program_id",
        field_type=UUIDType(),
        required=True,
        doc="Identifier of the study program offering the curriculum.",
    ),
    NestedField(
        field_id=3,
        name="curriculum_id",
        field_type=UUIDType(),
        required=True,
        doc="Identifier of the curriculum offered by the study program.",
    ),
)