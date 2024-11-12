from enum import StrEnum, Enum, auto


class UpperStrEnum(StrEnum):
    def _generate_next_value_(name, start, count, last_values):
        return name.upper()


class DatasetType(UpperStrEnum):
    STUDY_PROGRAMS: str = auto()
    CURRICULA: str = auto()
    COURSES: str = auto()
    MERGED_DATA: str = auto()


class StageType(UpperStrEnum):
    LOADING: str = auto()
    CLEANING: str = auto()
    VALIDATING: str = auto()
    EXTRACTING: str = auto()
    TRANSFORMING: str = auto()
    GENERATING: str = auto()
    FLATTENING: str = auto()
    MERGING: str = auto()
    STORING: str = auto()


class CoursePrerequisiteType(UpperStrEnum):
    REQUIRED_COURSE: str = auto()
    OPTIONAL_COURSES: str = auto()
    NO_PREREQUISITE: str = auto()
    MINIMUM_NUMBER_OF_COURSES_PASSED: str = auto()


class CourseSemesterSeasonType(UpperStrEnum):
    WINTER: str = auto()
    SUMMER: str = auto()
