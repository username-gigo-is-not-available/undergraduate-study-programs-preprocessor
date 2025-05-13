import logging
import time

import pandas as pd

from src.pipeline.course_pipeline import build_course_pipeline
from src.pipeline.offers_requires_pipeline import build_offers_requires_pipeline
from src.pipeline.professor_teaches_pipeline import build_professor_teaches_pipeline
from src.pipeline.study_program_pipeline import build_study_programs_pipeline

logging.basicConfig(level=logging.INFO)


def main() -> None:
    logging.info("Starting...")
    start: float = time.perf_counter()

    df_courses: pd.DataFrame = build_course_pipeline().run()
    build_professor_teaches_pipeline(df_courses).run()
    df_study_programs: pd.DataFrame = build_study_programs_pipeline().run()
    build_offers_requires_pipeline(df_study_programs, df_courses).run()
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")

if __name__ == '__main__':
    main()
