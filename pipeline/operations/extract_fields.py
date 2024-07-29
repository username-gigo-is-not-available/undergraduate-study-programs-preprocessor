from functools import cache

import patterns.singleton.instances.singleton_instances as singleton_instances
from utils.enums import CourseSeason, CoursePrerequisiteType


@cache
def extract_course_level(course_code: str) -> int:
    return int(course_code[4])


@cache
def extract_course_semester(course_academic_year: int, course_season: CourseSeason) -> int:
    return 2 * course_academic_year - 1 if course_season == CourseSeason.WINTER.value else 2 * course_academic_year


@cache
def extract_course_prerequisite_type(course_prerequisite: str,
                                     ) -> CoursePrerequisiteType:
    if course_prerequisite == 'нема':
        return CoursePrerequisiteType.NO_PREREQUISITE.value
    elif any(term in course_prerequisite for term in ['ЕКТС', 'ЕКСТ', 'кредити']):
        return CoursePrerequisiteType.NUMBER_OF_SUBJECTS_PASSED.value
    elif ' или ' in course_prerequisite:
        return CoursePrerequisiteType.MULTIPLE_COURSES.value
    elif singleton_instances.COURSE_NAMES_SINGLETON.get_most_similar_item(course_prerequisite) in singleton_instances.COURSE_NAMES_SINGLETON.get_items():
        return CoursePrerequisiteType.SINGLE_COURSE.value
    else:
        raise ValueError(f"Invalid course prerequisite: {course_prerequisite}")
