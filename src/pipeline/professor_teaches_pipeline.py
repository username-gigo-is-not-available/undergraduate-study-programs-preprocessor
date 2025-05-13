import pandas as pd

from src.config import Config
from src.field_parsers.clean_fields import clean_professor_titles, clean_and_format_field, clean_multivalued_field
from src.field_parsers.extract_fields import extract_professor_name, extract_professor_surname
from src.patterns.mixin.file_storage import FileStorageMixin
from src.pipeline.common_steps import clean_course_code_step, clean_course_name_mk_step
from src.pipeline.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def build_professor_teaches_pipeline(df_courses: pd.DataFrame) -> Pipeline:
    return (Pipeline(name='professor-teaches-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOADING)
        .add_step(
            PipelineStep(
                name='load-professor-teaches-data',
                function=PipelineStep.read_data,
                input_file_location=FileStorageMixin.get_input_file_location(),
                input_file_name=Config.COURSES_INPUT_DATA_FILE_PATH,
                column_order=Config.PROFESSOR_TEACHES_COLUMN_ORDER,
                drop_duplicates=True,
            )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEANING)
        .add_step(clean_course_code_step)
        .add_step(
            PipelineStep(
                name='clean-course-professors',
                function=PipelineStep.apply,
                mapping_function=clean_multivalued_field,
                source_columns='course_professors',
                destination_columns='course_professors',
            )
        )
    )
    .add_stage(
        PipelineStage(name='transform-data', stage_type=StageType.TRANSFORMING)
        .add_step(
            PipelineStep(
                name='flatten-course-professors',
                function=PipelineStep.explode,
                source_columns='course_professors',
                destination_columns='course_professors',
                drop_duplicates=True,
            )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEANING)
        .add_step(
            PipelineStep(
                name='clean-course-professor-titles',
                function=PipelineStep.apply,
                mapping_function=clean_professor_titles,
                source_columns='course_professors',
                destination_columns='course_professors',
            )
        )

    )
    .add_stage(
        PipelineStage(name='extract-data', stage_type=StageType.EXTRACTING)
        .add_step(
            PipelineStep(
                name='extract-professor-name',
                function=PipelineStep.apply,
                mapping_function=extract_professor_name,
                source_columns='course_professors',
                destination_columns='professor_name',
            )
        )
        .add_step(
            PipelineStep(
                name='extract-professor-surname',
                function=PipelineStep.apply,
                mapping_function=extract_professor_surname,
                source_columns='course_professors',
                destination_columns='professor_surname',
            )
        )
    )
    .add_stage(
        PipelineStage(name='merge-data', stage_type=StageType.MERGING)
        .add_step(
            PipelineStep(
                name='merge-with-course-data',
                function=PipelineStep.merge,
                merge_df=df_courses,
                on=['course_code'],
                how='inner'
            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATING)
        .add_step(
            PipelineStep(
                name='generate-professors-id',
                function=PipelineStep.uuid,
                source_columns=['professor_name', 'professor_surname'],
                destination_columns='professor_id',
            )
        )
        .add_step(
            PipelineStep(
                name='generate-teaches-id',
                function=PipelineStep.uuid,
                source_columns=['course_id', 'professor_id'],
                destination_columns='teaches_id',
            )
        )
    )
    .add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORING)
        .add_step(
            PipelineStep(
                name='store-professor-data',
                function=PipelineStep.save_data,
                output_file_location=FileStorageMixin.get_output_file_location(),
                output_file_name=Config.PROFESSORS_OUTPUT_FILE_NAME,
                column_order=Config.PROFESSORS_OUTPUT_COLUMN_ORDER,
                drop_duplicates=True,
                drop_na=True
            )
        )
        .add_step(
            PipelineStep(
                name='store-teaches-data',
                function=PipelineStep.save_data,
                output_file_location=FileStorageMixin.get_output_file_location(),
                output_file_name=Config.TEACHES_OUTPUT_FILE_NAME,
                column_order=Config.TEACHES_OUTPUT_COLUMN_ORDER,
                drop_duplicates=True
            )
        )
    )).build()
