import re
from decorators.field_sanitizers import sentence_case, clean_whitespace
from static import INVALID_COURSE_CODES_REGEX


def handle_invalid_course_rows(course_code: str | None = None,
                               course_name_mk: str | None = None,
                               course_name_en: str | None = None) -> tuple[str, ...]:
    result = []

    if re.search(INVALID_COURSE_CODES_REGEX, course_name_mk):
        result.extend(
            [
                handle_invalid_course_code(course_name_mk) if course_code is not None else None,
                handle_invalid_course_name(course_name_mk) if course_name_mk is not None else None,
                handle_invalid_course_name(course_name_en) if course_name_en is not None else None,
            ]
        )
    else:
        result.extend([course_code, course_name_mk, course_name_en])

    return tuple([item for item in result if item is not None])


@clean_whitespace
def handle_invalid_course_code(course_name: str) -> str:
    return ''.join(course_name.split(' ')[0]).upper()


@sentence_case
@clean_whitespace
def handle_invalid_course_name(course_name_mk: str) -> str:
    return ' '.join(course_name_mk.split(' ')[1:])
