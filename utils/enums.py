from enum import Enum


class CoursePrerequisiteType(Enum):
    SINGLE_COURSE: str = "SINGLE_COURSE"
    MULTIPLE_COURSES: str = "MULTIPLE_COURSES"
    NO_PREREQUISITE: str = "NO_PREREQUISITE"
    NUMBER_OF_SUBJECTS_PASSED: str = "NUMBER_OF_SUBJECTS_PASSED"


class CourseSeason(Enum):
    WINTER: str = "Зимски"
    SUMMER: str = "Летен"

