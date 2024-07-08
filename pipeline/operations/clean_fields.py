from functools import cache

from decorators.field_sanitizers import clean_whitespace, sentence_case, process_multivalued_field


@sentence_case
@clean_whitespace
@cache
def clean_study_program_name(study_program_name: str) -> str:
    return study_program_name


@clean_whitespace
@cache
def clean_course_code(course_code: str) -> str:
    return course_code


@sentence_case
@clean_whitespace
@cache
def clean_course_name_mk(course_name_mk: str) -> str:
    return course_name_mk


@sentence_case
@clean_whitespace
@cache
def clean_course_name_en(course_name_en: str) -> str:
    return course_name_en


@process_multivalued_field
@cache
def clean_course_professors(course_professors: str) -> str:
    return course_professors


@process_multivalued_field
@cache
def clean_course_prerequisites(course_prerequisites: str) -> str:
    return course_prerequisites
