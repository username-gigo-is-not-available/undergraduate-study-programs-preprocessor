import re
from functools import cache

import pandas as pd

from src.configurations import StorageConfiguration, ApplicationConfiguration
from src.patterns.strategy.data_frame import DataFrameStrategy
from src.pipeline.models.enums import CourseSemesterSeasonType, CoursePrerequisiteType


class ExtractionStrategy(DataFrameStrategy):
    def __init__(self, output_column: str) -> None:
        super().__init__()
        self.output_column = output_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement the apply method.")

    @classmethod
    def is_null(cls, value: str | None) -> bool:
        return str(value).lower().strip() in {'nan', 'нема', ''} or not value


class StudyProgramCodeStrategy(ExtractionStrategy):
    def __init__(self, url_column: str, duration_column: str, output_column: str) -> None:
        super().__init__(output_column)
        self.url_column = url_column
        self.duration_column = duration_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        def extract(url: str, duration: int) -> str:
            prefix: str = re.match(
                ApplicationConfiguration.STUDY_PROGRAM_CODE_REGEX, url.split('/')[-2]).group()
            return f"{prefix}{duration}"

        df[self.output_column] = df.apply(
            lambda row: extract(row[self.url_column], row[self.duration_column]),
            axis='columns'
        )
        return df


class CourseLevelStrategy(ExtractionStrategy):
    def __init__(self, code_column: str, output_column: str) -> None:
        super().__init__(output_column)
        self.code_column = code_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def extract(code: str) -> int:
            return int(code[4])

        df[self.output_column] = df[self.code_column].map(extract)
        return df


class CourseAcademicYearStrategy(ExtractionStrategy):
    def __init__(self, semester_column: str, output_column: str) -> None:
        super().__init__(output_column)
        self.semester_column = semester_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def extract(semester: int) -> int:
            return (semester + 1) // 2

        df[self.output_column] = df[self.semester_column].map(extract)
        return df


class CourseSemesterSeasonStrategy(ExtractionStrategy):
    def __init__(self, semester_column: str, output_column: str) -> None:
        super().__init__(output_column)
        self.semester_column = semester_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def extract(semester: int) -> CourseSemesterSeasonType:
            if semester % 2 == 1:
                return CourseSemesterSeasonType.WINTER
            return CourseSemesterSeasonType.SUMMER
        df[self.output_column] = df[self.semester_column].map(extract)
        return df


class CoursePrerequisiteTypeStrategy(ExtractionStrategy):
    def __init__(self, prerequisite_column: str, output_column: str) -> None:
        super().__init__(output_column)
        self.prerequisite_column = prerequisite_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def extract(prerequisite: str) -> str:
            if ExtractionStrategy.is_null(prerequisite):
                return CoursePrerequisiteType.NONE
            elif any(term in prerequisite for term in ['ЕКТС', 'ЕКСТ', 'кредити']):
                return CoursePrerequisiteType.TOTAL
            elif "|" in prerequisite:
                return CoursePrerequisiteType.ANY
            else:
                return CoursePrerequisiteType.ONE

        df[self.output_column] = df[self.prerequisite_column].map(extract)
        return df


class MinimumNumberOfCoursesStrategy(ExtractionStrategy):
    def __init__(self, prerequisite_column: str, prerequisite_type_column: str, output_column: str) -> None:
        super().__init__(output_column)
        self.prerequisite_column = prerequisite_column
        self.prerequisite_type_column = prerequisite_type_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def extract(prerequisite: str, prerequisite_type: CoursePrerequisiteType) -> int:
            if prerequisite_type == CoursePrerequisiteType.TOTAL:
                return int(re.search(r'(\d+)', prerequisite).group()) // ApplicationConfiguration.ECTS_VALUE
            return 0

        df[self.output_column] = df.apply(
            lambda row: extract(row[self.prerequisite_column], row[self.prerequisite_type_column]), axis='columns'
        )
        return df


class ProfessorNameStrategy(ExtractionStrategy):
    def __init__(self, full_name_column: str, output_column: str) -> None:
        super().__init__(output_column)
        self.full_name_column = full_name_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def extract(full_name: str) -> str | None:
            if ExtractionStrategy.is_null(full_name):
                return None
            return full_name.split(' ')[0]

        df[self.output_column] = df[self.full_name_column].map(extract)
        return df


class ProfessorSurnameStrategy(ExtractionStrategy):
    def __init__(self, full_name_column: str, output_column: str) -> None:
        super().__init__(output_column)
        self.full_name_column = full_name_column

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        @cache
        def extract(full_name: str) -> str | None:
            if ExtractionStrategy.is_null(full_name):
                return None
            return ' '.join(full_name.split(' ')[1:])

        df[self.output_column] = df[self.full_name_column].map(extract)
        return df
