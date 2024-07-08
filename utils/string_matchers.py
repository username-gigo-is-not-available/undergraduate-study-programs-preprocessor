from difflib import SequenceMatcher

from static import COURSE_NAMES


def get_most_similar_course_name(prerequisite: str,) -> str:
    return max(COURSE_NAMES, key=lambda course_name: SequenceMatcher(None, prerequisite, course_name).ratio())
