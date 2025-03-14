import logging
import time

from src.pipeline.course_professor_pipeline import build_course_professor_pipeline
from src.pipeline.curriculum_prerequisite_pipeline import build_curriculum_prerequisites_pipeline
from src.pipeline.study_program_pipeline import build_study_programs_pipeline

logging.basicConfig(level=logging.INFO)


def main() -> None:
    logging.info("Starting...")
    start: float = time.perf_counter()

    build_course_professor_pipeline().run()
    build_study_programs_pipeline().run()
    build_curriculum_prerequisites_pipeline().run()
    logging.info(f"Time taken: {time.perf_counter() - start:.2f} seconds")


if __name__ == '__main__':
    main()
