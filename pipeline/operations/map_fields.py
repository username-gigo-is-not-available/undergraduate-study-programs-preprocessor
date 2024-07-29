from functools import cache

import patterns.singleton.instances.singleton_instances as singleton_instances
from utils.enums import CoursePrerequisiteType


@cache
def map_course_prerequisites_to_ids(course_prerequisite_type: str,
                                    course_prerequisites: str) -> str:
    if course_prerequisite_type in [CoursePrerequisiteType.MULTIPLE_COURSES.value, CoursePrerequisiteType.SINGLE_COURSE.value]:
        course_prerequisites_ids: list[int] = list(map(singleton_instances.COURSE_NAMES_SINGLETON.get_index_of_item, course_prerequisites.split(", ")))
        return ', '.join(sorted(map(str, course_prerequisites_ids)))

    elif course_prerequisite_type in [CoursePrerequisiteType.NO_PREREQUISITE.value, CoursePrerequisiteType.NUMBER_OF_SUBJECTS_PASSED.value]:
        return "нема"

    else:
        raise ValueError(f"Invalid course prerequisite type: {course_prerequisite_type}")


@cache
def map_course_professors_to_ids(course_professors: str) -> str:
    course_professors_ids: list[int] = list(map(singleton_instances.COURSE_PROFESSORS_SINGLETON.get_index_of_item, course_professors.split(", ")))
    return ', '.join(sorted(map(str, course_professors_ids))) if course_professors_ids else "нема"
