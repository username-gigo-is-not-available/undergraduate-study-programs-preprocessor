from difflib import SequenceMatcher

from src.pipeline.models.enums import CoursePrerequisiteType


def transform_course_prerequisites(course_prerequisite_type: CoursePrerequisiteType,
                                   course_prerequisites: str,
                                   course_name_mk: str,
                                   course_names: list[str]
                                   ) -> str:
    def get_most_similar_course_prerequisite(course_prerequisite: str,
                                             course_names: list[str]) -> str:
        similarity = {}
        for course in course_names:
            ratio = SequenceMatcher(None, course_prerequisite, course).ratio()
            if ratio >= 0.835:
                similarity[course] = ratio

        return max(similarity, key=lambda k: similarity[k]) if similarity else None

    if course_prerequisite_type == CoursePrerequisiteType.ONE:

        course_prerequisite = get_most_similar_course_prerequisite(course_prerequisite=course_prerequisites.strip(),
                                                                   course_names=course_names)
    elif course_prerequisite_type == CoursePrerequisiteType.ANY:
        course_prerequisites_list = []
        for course in course_prerequisites.split("|"):
            most_similar_course_prerequisite = get_most_similar_course_prerequisite(course.strip(), course_names)
            if most_similar_course_prerequisite:
                course_prerequisites_list.append(most_similar_course_prerequisite)

        course_prerequisite = "|".join(course_prerequisites_list) if course_prerequisites_list else None

    elif course_prerequisite_type == CoursePrerequisiteType.TOTAL:
        course_prerequisite = "|".join([course_name for course_name in course_names if course_name != course_name_mk])

    elif course_prerequisite_type == CoursePrerequisiteType.NONE:
        course_prerequisite = 'нема'
    else:
        raise ValueError(f"Invalid course prerequisite type: {course_prerequisite_type}")
    return course_prerequisite
