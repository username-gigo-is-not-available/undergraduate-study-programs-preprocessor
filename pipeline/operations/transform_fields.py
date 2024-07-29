import re
from functools import cache

from utils.enums import CoursePrerequisiteType
from static import ECTS_VALUE, PROFESSOR_TITLES
import patterns.singleton.instances.singleton_instances as singleton_instances


@cache
def transform_course_prerequisites(course_prerequisite_type: str,
                                   course_prerequisites: str) -> str:
    if course_prerequisite_type in [CoursePrerequisiteType.MULTIPLE_COURSES.value, CoursePrerequisiteType.SINGLE_COURSE.value]:
        course_prerequisite = ", ".join(
            sorted(map(lambda cp: singleton_instances.COURSE_NAMES_SINGLETON.get_most_similar_item(cp.strip()),
                       course_prerequisites.split(' или ')))
        )

    elif course_prerequisite_type == CoursePrerequisiteType.NUMBER_OF_SUBJECTS_PASSED.value:
        course_prerequisite = str(int(re.search(r'(\d+)', course_prerequisites).group()) // ECTS_VALUE)

    elif course_prerequisite_type == CoursePrerequisiteType.NO_PREREQUISITE.value:
        course_prerequisite = 'нема'
    else:
        raise ValueError(f"Invalid course prerequisite type: {course_prerequisite_type}")

    return course_prerequisite


@cache
def transform_course_professors(course_professors: str) -> str:
    def parse_professor_title(professor_name: str) -> str:
        for title in PROFESSOR_TITLES:
            professor_name = professor_name.replace(title, "").strip()
        return professor_name

    return ', '.join(sorted((map(parse_professor_title, course_professors.split(", ")))))
