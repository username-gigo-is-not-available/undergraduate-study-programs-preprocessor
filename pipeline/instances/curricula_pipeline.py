import pandas as pd

from pipeline.model.pipeline import Pipeline
from pipeline.operations.handle_invalid_fields import handle_invalid_course_rows
from pipeline.model.steps import PipelineStep
from pipeline.operations.clean_fields import clean_study_program_name, clean_course_code, clean_course_name_mk

curricula_clean_data_pipeline = Pipeline(
    name='clean_curricula_data',
    steps=[
        PipelineStep(name='clean_study_program_name', function=clean_study_program_name, source_columns=['study_program_name'],
                     destination_columns=['study_program_name']),
        PipelineStep(name='clean_course_code', function=clean_course_code, source_columns=['course_code'],
                     destination_columns=['course_code']),
        PipelineStep(name='clean_course_name_mk', function=clean_course_name_mk, source_columns=['course_name_mk'],
                     destination_columns=['course_name_mk']),
    ]
)

curricula_handle_invalid_data_pipeline = Pipeline(
    name='handle_invalid_curricula_data',
    steps=[
        PipelineStep(name='handle_invalid_course_rows', function=handle_invalid_course_rows,
                     source_columns=['course_code', 'course_name_mk'],
                     destination_columns=['course_code', 'course_name_mk']),
    ]
)


def run_curricula_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    curricula_clean_data_pipeline.run(df)
    curricula_handle_invalid_data_pipeline.run(df)
    return df
