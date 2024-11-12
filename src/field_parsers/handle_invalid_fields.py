import re

from src.config import Config


def validate_course_rows(course_code: str | None = None,
                         course_name_mk: str | None = None,
                         course_name_en: str | None = None) -> tuple[str]:
    result = []

    is_invalid: bool = bool(course_name_mk and re.search(Config.COURSE_CODES_REGEX, course_name_mk))

    result.extend([
        handle_invalid_course_code(course_name_mk) if is_invalid and course_code else course_code,
        handle_invalid_course_name(course_name_mk) if is_invalid and course_name_mk else course_name_mk,
        handle_invalid_course_name(course_name_en) if is_invalid and course_name_en else course_name_en
    ])

    return tuple(filter(None, result))


def handle_invalid_course_code(course_name: str) -> str:
    return ''.join(course_name.split(' ')[0]).upper()


def handle_invalid_course_name(course_name_mk: str) -> str:
    return ' '.join(course_name_mk.split(' ')[1:])
