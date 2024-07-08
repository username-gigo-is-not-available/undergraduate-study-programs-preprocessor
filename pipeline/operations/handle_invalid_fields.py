from decorators.field_sanitizers import sentence_case, clean_whitespace
from static import INVALID_COURSE_CODES


def handle_invalid_course_rows(course_code: str, course_name_mk: str, course_name_en: str | None = None) -> (tuple[str, str, str]
                                                                                                             | tuple[str, str]):

    if course_code not in INVALID_COURSE_CODES:
        return (course_code, course_name_mk, course_name_en) if course_name_en else (course_code, course_name_mk)

    new_course_code = handle_invalid_course_code(course_name_mk)
    new_course_name_mk = handle_invalid_course_name(course_name_mk)
    new_course_name_en = handle_invalid_course_name(course_name_en) if course_name_en else None

    return (new_course_code, new_course_name_mk, new_course_name_en) if new_course_name_en else (new_course_code, new_course_name_mk)


@clean_whitespace
def handle_invalid_course_code(course_name: str) -> str:
    return ''.join(course_name.split(' ')[0]).upper()


@sentence_case
@clean_whitespace
def handle_invalid_course_name(course_name_mk: str) -> str:
    return ' '.join(course_name_mk.split(' ')[1:])
