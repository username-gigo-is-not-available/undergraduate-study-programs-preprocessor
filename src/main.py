import logging
import time
from pathlib import Path

import pandas as pd

from src.configurations import DatasetConfiguration
from src.pipeline.course_pipeline import course_pipeline
from src.pipeline.curriculum_pipeline import curriculum_pipeline
from src.pipeline.models.enums import DatasetType
from src.pipeline.professor_pipeline import professor_pipeline
from src.pipeline.requisite_pipeline import requisite_pipeline
from src.pipeline.study_program_pipeline import study_programs_pipeline

logging.basicConfig(level=logging.INFO)


def main() -> None:
    logging.info("Starting...")
    start: float = time.perf_counter()
    datasets: dict[DatasetType, DatasetConfiguration] = DatasetConfiguration.initialize()
    df_study_programs: pd.DataFrame = study_programs_pipeline(datasets[DatasetType.STUDY_PROGRAMS]).build().run()
    df_courses: pd.DataFrame = course_pipeline(datasets[DatasetType.COURSES]).build().run()
    professor_pipeline(datasets[DatasetType.PROFESSORS], datasets[DatasetType.TEACHES], df_courses).build().run()
    curriculum_pipeline(datasets[DatasetType.CURRICULA], datasets[DatasetType.OFFERS], datasets[DatasetType.INCLUDES],
        df_study_programs, df_courses).build().run()
    requisite_pipeline(
        datasets[DatasetType.REQUISITES],
        datasets[DatasetType.PREREQUISITES],
        datasets[DatasetType.POSTREQUISITES],
        df_courses).build().run()
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")

if __name__ == '__main__':
    main()
