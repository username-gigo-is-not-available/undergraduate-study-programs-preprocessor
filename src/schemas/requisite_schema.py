from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType,
    UUIDType,
    IntegerType,
)
from pyiceberg.schema import NestedField

REQUISITE_SCHEMA = Schema(
    NestedField(
        field_id=1,
        name="requisite_id",
        field_type=UUIDType(),
        required=True,
        doc="Universally unique identifier (UUID) of the requisite",
    ),
    NestedField(
        field_id=2,
        name="course_prerequisite_type",
        field_type=StringType(),
        required=True,
        doc="The logical type of prerequisite condition required for enrollment. (ONE, ANY, or TOTAL)",
    ),
    NestedField(
        field_id=3,
        name="minimum_required_number_of_courses",
        field_type=IntegerType(),
        required=True,
        doc="Minimum number of prerequisite courses that must be passed to satisfy the condition (range: [0, 34]).",
    ),
)