from difflib import SequenceMatcher
from functools import cache
import pandas as pd

from src.config import Config
from src.patterns.strategy.data_frame import DataFrameStrategy
from src.pipeline.models.enums import CoursePrerequisiteType


class MatchingStrategy(DataFrameStrategy):

    def __init__(self, column: str, truth_column: str) -> None:
        super().__init__()
        self.column = column
        self.truth_column = truth_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement the apply method.")

    @staticmethod
    def get_most_similar_string(arg: str, values: list[str]) -> str:
        similarity = {}
        for value in values:
            similarity[value] = SequenceMatcher(None, arg, value).ratio()
            if similarity[value] == Config.MAXIMUM_SIMILARITY_RATIO:
                return value

        return max(similarity, key=lambda k: similarity[k]) if similarity else None


class CoursePrerequisiteStrategy(MatchingStrategy):
    def __init__(self, column: str, truth_column: str, prerequisite_type_column: str, delimiter: str) -> None:
        super().__init__(column, truth_column)
        self.prerequisite_type_column = prerequisite_type_column
        self.delimiter = delimiter

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def match(course_name_mk: str, prerequisite_course_name: str, prerequisite_type: CoursePrerequisiteType, course_names: list[str]) -> str:

            if prerequisite_type == CoursePrerequisiteType.ONE:

                course_prerequisite = MatchingStrategy.get_most_similar_string(
                    prerequisite_course_name.strip(),
                    course_names
                )

            elif prerequisite_type == CoursePrerequisiteType.ANY:
                course_prerequisites_list = [
                    MatchingStrategy.get_most_similar_string(course.strip(), course_names)
                    for course in prerequisite_course_name.split(self.delimiter)
                ]

                course_prerequisite = self.delimiter.join(
                    course_prerequisites_list) if course_prerequisites_list else None

            elif prerequisite_type == CoursePrerequisiteType.TOTAL:
                course_prerequisite = self.delimiter.join(
                    [course_name for course_name in course_names if course_name != course_name_mk])

            elif prerequisite_type == CoursePrerequisiteType.NONE:
                course_prerequisite = None
            else:
                raise ValueError(f"Invalid course prerequisite type: {prerequisite_type}")
            return course_prerequisite

        courses: list[str] = [row for row in df[self.truth_column].drop_duplicates().values.tolist()]
        df[self.column] = df.apply(
            lambda row: match(row[self.truth_column], row[self.column], row[self.prerequisite_type_column],
                              tuple(courses)), axis='columns')
        return df
