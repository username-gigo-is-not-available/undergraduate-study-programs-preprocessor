import re
from functools import cache

from utils.enums import CoursePrerequisiteType
from utils.string_matchers import get_most_similar_course_name
from static import ECTS_VALUE, PROFESSOR_TITLES


@cache
def transform_course_prerequisites(course_prerequisite_type: str,
                                   course_prerequisites: str) -> str:

    if course_prerequisite_type == CoursePrerequisiteType.MULTIPLE_COURSES.value or \
            course_prerequisite_type == CoursePrerequisiteType.SINGLE_COURSE.value:
        result = ", ".join(
            sorted(map(lambda cp: get_most_similar_course_name(cp.strip()),
                       course_prerequisites.split(' или ')))
        )

    elif course_prerequisite_type == CoursePrerequisiteType.NUMBER_OF_SUBJECTS_PASSED.value:
        result = str(int(re.search(r'(\d+)', course_prerequisites).group()) // ECTS_VALUE)

    elif course_prerequisite_type == CoursePrerequisiteType.NO_PREREQUISITE.value:
        result = 'нема'
    else:
        raise ValueError(f"Invalid course prerequisite type: {course_prerequisite_type}")

    return result


@cache
def transform_course_professors(course_professors: str) -> str:

    def parse_professor_title(professor_name: str) -> str:
        for title in PROFESSOR_TITLES:
            professor_name = professor_name.replace(title, "").strip()
        return professor_name

    return ', '.join(sorted((map(parse_professor_title, course_professors.split(", ")))))
