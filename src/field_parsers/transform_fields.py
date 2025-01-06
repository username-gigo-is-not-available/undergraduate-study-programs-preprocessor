from difflib import SequenceMatcher
from functools import cache

from src.pipeline.models.enums import CoursePrerequisiteType


@cache
def transform_course_prerequisites(course_prerequisite_type: CoursePrerequisiteType,
                                   course_prerequisites: str,
                                   course_name_mk: str,
                                   course_names: list[str]
                                   ) -> str:
    def get_most_similar_course_prerequisite(course_prerequisite: str,
                                   course_names: list[str]) -> str:
        return max(course_names, key=lambda course_name: SequenceMatcher(None, course_prerequisite, course_name).ratio())

    if course_prerequisite_type in [CoursePrerequisiteType.OPTIONAL_COURSES, CoursePrerequisiteType.REQUIRED_COURSE]:
        course_prerequisite = "|".join(
            sorted(map(lambda cp: get_most_similar_course_prerequisite(cp.strip(), course_names),
                       course_prerequisites.split(' или ')))
        )

    elif course_prerequisite_type == CoursePrerequisiteType.MINIMUM_NUMBER_OF_COURSES_PASSED:
        course_prerequisite = "|".join([course_name for course_name in course_names if course_name != course_name_mk])

    elif course_prerequisite_type == CoursePrerequisiteType.NO_PREREQUISITE:
        course_prerequisite = 'нема'
    else:
        raise ValueError(f"Invalid course prerequisite type: {course_prerequisite_type}")

    return course_prerequisite


