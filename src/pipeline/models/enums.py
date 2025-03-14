from enum import StrEnum, Enum, auto


class UpperStrEnum(StrEnum):
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()


class StageType(UpperStrEnum):
    LOADING: str = auto()
    CLEANING: str = auto()
    EXTRACTING: str = auto()
    TRANSFORMING: str = auto()
    GENERATING: str = auto()
    FLATTENING: str = auto()
    MERGING: str = auto()
    STORING: str = auto()


class CoursePrerequisiteType(UpperStrEnum):
    NONE: str = auto()
    ONE: str = auto()
    ANY: str = auto()
    TOTAL: str = auto()


class CourseSemesterSeasonType(UpperStrEnum):
    WINTER: str = auto()
    SUMMER: str = auto()
