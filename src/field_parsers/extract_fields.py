import math
import re
from functools import cache

from src.pipeline.models.enums import CourseSemesterSeasonType, CoursePrerequisiteType
from src.config import Config


@cache
def extract_study_program_code(study_program_url: str, study_program_duration: int) -> str:
    study_program_code: str = re.match(Config.STUDY_PROGRAM_CODE_REGEX, study_program_url.split('/')[-2]).group(0)
    return ''.join([study_program_code, str(study_program_duration)])


@cache
def extract_course_level(course_code: str) -> int:
    return int(course_code[4])


@cache
def extract_course_academic_year(course_semester: int) -> int:
    return math.ceil(course_semester / 2)


@cache
def extract_course_semester_season(course_semester: int) -> CourseSemesterSeasonType:
    return CourseSemesterSeasonType.WINTER if course_semester % 2 == 1 else CourseSemesterSeasonType.SUMMER


@cache
def extract_course_prerequisite_type(course_prerequisite: str,
                                     ) -> CoursePrerequisiteType:
    if course_prerequisite == 'нема':
        return CoursePrerequisiteType.NONE
    elif any(term in course_prerequisite for term in ['ЕКТС', 'ЕКСТ', 'кредити']):
        return CoursePrerequisiteType.TOTAL
    elif "|" in course_prerequisite:
        return CoursePrerequisiteType.ANY
    else:
        return CoursePrerequisiteType.ONE


@cache
def extract_minimum_number_of_courses_passed(course_prerequisite_type: CoursePrerequisiteType, course_prerequisite: str) -> int:
    if course_prerequisite_type == CoursePrerequisiteType.TOTAL:
        return int(re.search(r'(\d+)', course_prerequisite).group()) // Config.ECTS_VALUE
    return 0


@cache
def extract_professor_name(course_professor: str) -> str:
    return course_professor.split(' ')[0]


@cache
def extract_professor_surname(course_professor: str) -> str:
    return ' '.join(course_professor.split(' ')[1:])
