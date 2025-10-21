import time
import pandas as pd
import logging


from src.initialization import initialize
from src.pipeline.course_pipeline import course_pipeline
from src.pipeline.curriculum_pipeline import curriculum_pipeline
from src.pipeline.professor_pipeline import professor_pipeline
from src.pipeline.requisite_pipeline import requisite_pipeline
from src.pipeline.study_program_pipeline import study_programs_pipeline

logging.basicConfig(level=logging.INFO, force=True)

def main() -> None:
    logging.info("Starting...")
    start: float = time.perf_counter()
    initialize()
    df_study_programs: pd.DataFrame = study_programs_pipeline().build().run()
    df_courses: pd.DataFrame = course_pipeline().build().run()
    professor_pipeline(df_courses).build().run()
    df_requisites: pd.DataFrame = requisite_pipeline(df_courses).build().run()
    curriculum_pipeline(df_study_programs, df_courses, df_requisites).build().run()
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    main()
