from enum import StrEnum, auto


class UpperStrEnum(StrEnum):
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()


class StageType(UpperStrEnum):
    LOAD: str = auto()
    SELECT: str = auto()
    CLEAN: str = auto()
    FILTER: str = auto()
    EXTRACT: str = auto()
    TRANSFORM: str = auto()
    GENERATE: str = auto()
    FLATTEN: str = auto()
    MERGE: str = auto()
    VALIDATE = auto()
    STORE: str = auto()


class CoursePrerequisiteType(UpperStrEnum):
    NONE: str = auto()
    ONE: str = auto()
    ANY: str = auto()
    TOTAL: str = auto()

class CourseSemesterSeasonType(UpperStrEnum):
    WINTER: str = auto()
    SUMMER: str = auto()

class DatasetType(UpperStrEnum):
    STUDY_PROGRAMS: str = auto()
    COURSES: str = auto()
    PROFESSORS: str = auto()
    CURRICULA: str = auto()
    REQUISITES: str = auto()
    OFFERS: str = auto()
    INCLUDES: str = auto()
    REQUIRES: str = auto()
    SATISFIES: str = auto()
    TEACHES: str = auto()