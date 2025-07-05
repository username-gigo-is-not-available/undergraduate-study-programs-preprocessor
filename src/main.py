import logging
import time

import pandas as pd

from src.pipeline.course_pipeline import course_pipeline
from src.pipeline.curriculum_pipeline import curriculum_pipeline
from src.pipeline.professor_pipeline import professor_pipeline
from src.pipeline.requisite_pipeline import requisite_pipeline
from src.pipeline.study_program_pipeline import study_programs_pipeline

logging.basicConfig(level=logging.INFO)


def main() -> None:
    logging.info("Starting...")
    start: float = time.perf_counter()
    df_study_programs: pd.DataFrame = study_programs_pipeline().build().run()
    df_courses: pd.DataFrame = course_pipeline().build().run()
    professor_pipeline(df_courses).build().run()
    curriculum_pipeline(df_study_programs, df_courses).build().run()
    requisite_pipeline(df_courses).build().run()
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    main()
