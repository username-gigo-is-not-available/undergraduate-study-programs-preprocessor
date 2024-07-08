import pandas as pd

from pipeline.model.pipeline import Pipeline
from pipeline.model.steps import PipelineStep
from pipeline.operations.clean_fields import clean_course_code, clean_course_name_mk, clean_course_name_en, clean_course_professors, \
    clean_course_prerequisites
from pipeline.operations.extract_fields import extract_course_level, extract_course_semester, extract_course_prerequisite_type
from pipeline.operations.handle_invalid_fields import handle_invalid_course_rows
from pipeline.operations.transform_fields import transform_course_professors, transform_course_prerequisites

courses_clean_data_pipeline = Pipeline(
    name='clean_courses_data',
    steps=[
        PipelineStep(name='clean_course_code', function=clean_course_code, source_columns=['course_code'],
                     destination_columns=['course_code']),
        PipelineStep(name='clean_course_name_mk', function=clean_course_name_mk, source_columns=['course_name_mk'],
                     destination_columns=['course_name_mk']),
        PipelineStep(name='clean_course_name_en', function=clean_course_name_en, source_columns=['course_name_en'],
                     destination_columns=['course_name_en']),
        PipelineStep(name='clean_course_professors', function=clean_course_professors, source_columns=['course_professors'],
                     destination_columns=['course_professors']),
        PipelineStep(name='clean_course_prerequisites', function=clean_course_prerequisites, source_columns=['course_prerequisite'],
                     destination_columns=['course_prerequisite']),
    ]
)

courses_handle_invalid_data_pipeline = Pipeline(
    name='handle_invalid_course_data',
    steps=[
        PipelineStep(name='handle_invalid_course_rows', function=handle_invalid_course_rows,
                     source_columns=['course_code', 'course_name_mk', 'course_name_en'],
                     destination_columns=['course_code', 'course_name_mk', 'course_name_en']),
    ]
)

courses_extract_data_pipeline = Pipeline(
    name='extract_course_data',
    steps=[
        PipelineStep(name='extract_course_level', function=extract_course_level, source_columns=['course_code'],
                     destination_columns=['course_level']),
        PipelineStep(name='extract_course_semester', function=extract_course_semester,
                     source_columns=['course_academic_year', 'course_season'],
                     destination_columns=['course_semester']),
        PipelineStep(name='extract_course_prerequisite_type', function=extract_course_prerequisite_type,
                     source_columns=['course_prerequisite'],
                     destination_columns=['course_prerequisite_type']),

    ]
)

courses_transform_data_pipeline = Pipeline(
    name='transform_course_data',
    steps=[
        PipelineStep(name='transform_course_professors', function=transform_course_professors, source_columns=['course_professors'],
                     destination_columns=['course_professors']),
        PipelineStep(name='transform_course_prerequisites', function=transform_course_prerequisites,
                     source_columns=['course_prerequisite_type', 'course_prerequisite'], destination_columns=['course_prerequisite']),
    ]
)


def run_courses_pipeline(df: pd.DataFrame) -> None:
    courses_clean_data_pipeline.run(df)
    courses_handle_invalid_data_pipeline.run(df)
    courses_extract_data_pipeline.run(df)
    courses_transform_data_pipeline.run(df)
