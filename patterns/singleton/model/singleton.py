import threading
from abc import ABC, abstractmethod
from difflib import SequenceMatcher

import pandas as pd

from pipeline.operations.clean_fields import clean_course_name_mk, clean_course_professors
from pipeline.operations.handle_invalid_fields import handle_invalid_course_rows
from pipeline.operations.transform_fields import transform_course_professors
from static import COURSES_INPUT_DATA_FILE_PATH


class AbstractSingleton(ABC):
    _instance: 'AbstractSingleton' = None
    _items: list[str] = []
    _lock: threading.Lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(AbstractSingleton, cls).__new__(cls)
                cls._initialize()
            return cls._instance

    @classmethod
    @abstractmethod
    def _initialize(cls):
        pass

    @classmethod
    def get_items(cls) -> list[str]:
        return cls._items

    @classmethod
    def get_most_similar_item(self, item: str) -> str:
        return max(self.get_items(), key=lambda course_name: SequenceMatcher(None, item, course_name).ratio())

    @classmethod
    def get_index_of_item(cls, item: str) -> int:
        return cls._items.index(item)


class CourseNamesSingleton(AbstractSingleton):
    @classmethod
    def _initialize(cls):
        cls._items: list[str] = list(
            pd.read_csv(COURSES_INPUT_DATA_FILE_PATH)
            .get('course_name_mk')
            .apply(lambda x: list(handle_invalid_course_rows(course_name_mk=x)).pop())
            .apply(clean_course_name_mk)
            .sort_values()
        )


class CourseProfessorsSingleton(AbstractSingleton):
    @classmethod
    def _initialize(cls):
        cls._items: list[str] = list(
            pd.read_csv(COURSES_INPUT_DATA_FILE_PATH)
            .get('course_professors')
            .apply(clean_course_professors)
            .apply(transform_course_professors)
            .apply(lambda x: x.split(", "))
            .explode()
            .drop_duplicates()
        )
