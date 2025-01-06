from functools import cache

from src.field_parsers.text_sanitizing_decorators import clean_whitespace, sentence_case, process_multivalued_field
from src.config import Config


@sentence_case
@clean_whitespace
@cache
def clean_and_format_field(field: str) -> str:
    return field


@clean_whitespace
@cache
def clean_field(field: str) -> str:
    return field


@process_multivalued_field
@cache
def clean_and_format_multivalued_field(field: str) -> str:
    return field


@process_multivalued_field
@cache
def clean_professor_titles(course_professors: str) -> str:
    def parse_professor_title(professor_name: str) -> str:
        for title in Config.PROFESSOR_TITLES:
            professor_name = professor_name.replace(title, "").strip()
        return professor_name

    return "|".join(sorted((map(parse_professor_title, str(course_professors).split(", ")))))
