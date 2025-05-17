from src.config import Config
from src.field_parsers.clean_fields import clean_and_format_field
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep
from src.pipeline.common_steps import clean_course_code_step, clean_course_name_mk_step
from src.pipeline.models.enums import StageType


def course_pipeline() -> Pipeline:
    return (Pipeline(name='course-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOADING)
        .add_step(
            PipelineStep(
                name='load-course-data',
                function=PipelineStep.read_data,
                input_file_location=PipelineStep.get_input_file_location(),
                input_file_name=Config.COURSES_INPUT_DATA_FILE_PATH,
                column_order=Config.COURSES_INPUT_COLUMN_ORDER,
                drop_duplicates=True,
            )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEANING)
        .add_step(clean_course_code_step)
        .add_step(clean_course_name_mk_step)
        .add_step(
            PipelineStep(
                name='clean-course-name-en',
                function=PipelineStep.apply,
                mapping_function=clean_and_format_field,
                source_columns='course_name_en',
                destination_columns='course_name_en',
            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATING)
        .add_step(
            PipelineStep(
                name='generate-course-id',
                function=PipelineStep.uuid,
                source_columns='course_name_mk',
                destination_columns='course_id',
            )
        )
    )
    .add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORING)
        .add_step(
            PipelineStep(
                name='store-course-data',
                function=PipelineStep.save_data,
                output_file_location=PipelineStep.get_output_file_location(),
                output_file_name=Config.COURSES_OUTPUT_FILE_NAME,
                column_order=Config.COURSES_OUTPUT_COLUMN_ORDER,
                drop_duplicates=True,
            )
        )
    ))
