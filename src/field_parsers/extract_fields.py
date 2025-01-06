import re
from functools import cache

from src.pipeline.models.enums import CourseSemesterSeasonType, CoursePrerequisiteType
from src.config import Config


def extract_study_program_code(study_program_url: str, study_program_duration: int) -> str:
    study_program_code: str = re.match(Config.STUDY_PROGRAM_CODE_REGEX, study_program_url.split('/')[-2]).group(0)
    return ''.join([study_program_code, str(study_program_duration)])


def extract_course_level(course_code: str) -> int:
    return int(course_code[4])


@cache
def extract_course_semester(course_academic_year: int, course_season: CourseSemesterSeasonType) -> int:
    return 2 * course_academic_year - 1 if course_season == CourseSemesterSeasonType.WINTER.value else 2 * course_academic_year


@cache
def extract_course_prerequisite_type(course_prerequisite: str,
                                     ) -> CoursePrerequisiteType:
    if course_prerequisite == 'нема':
        return CoursePrerequisiteType.NO_PREREQUISITE
    elif any(term in course_prerequisite for term in ['ЕКТС', 'ЕКСТ', 'кредити']):
        return CoursePrerequisiteType.MINIMUM_NUMBER_OF_COURSES_PASSED
    elif ' или ' in course_prerequisite:
        return CoursePrerequisiteType.OPTIONAL_COURSES
    else:
        return CoursePrerequisiteType.REQUIRED_COURSE


@cache
def extract_minimum_number_of_courses_passed(course_prerequisite_type: CoursePrerequisiteType, course_prerequisites: str) -> int:
    if course_prerequisite_type == CoursePrerequisiteType.MINIMUM_NUMBER_OF_COURSES_PASSED:
        return int(re.search(r'(\d+)', course_prerequisites).group()) // Config.ECTS_VALUE
    else:
        return 0
