from src.pipeline.models.enums import DatasetType, StageType
from src.pipeline.common_steps import clean_course_code_step, clean_course_name_mk_step, clean_study_program_name_step
from src.pipeline.models.pipeline import Pipeline
from src.patterns.builder.pipeline_builder import PipelineBuilder
from src.patterns.builder.pipeline_stage_builder import PipelineStageBuilder
from src.pipeline.models.step import PipelineStep
from src.field_parsers.handle_invalid_fields import validate_course_rows
from src.config import Config


def build_curricula_pipeline(stages: list[StageType] = tuple(StageType)) -> Pipeline:
    builder: PipelineBuilder = PipelineBuilder(name='curricula_pipeline', dataset_type=DatasetType.CURRICULA)
    # Loading stage
    if StageType.LOADING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='load_curricula_data', stage_type=StageType.LOADING)
            .add_step(PipelineStep(
                name='load_curricula_data',
                function=PipelineStep.read_data,
                input_file_name=Config.CURRICULA_INPUT_DATA_FILE_PATH,
            ))
        )
    # Cleaning Stage
    if StageType.CLEANING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='clean_curricula_data', stage_type=StageType.CLEANING)
            .add_step(clean_study_program_name_step)
            .add_step(clean_course_code_step)
            .add_step(clean_course_name_mk_step)
        )
    # Validating Stage
    if StageType.VALIDATING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='validate_curricula_data', stage_type=StageType.VALIDATING)
            .add_step(PipelineStep(
                name='validate_curricula_data',
                function=PipelineStep.apply_function,
                mapping_function=validate_course_rows,
                source_columns=['course_code', 'course_name_mk'],
                destination_columns=['course_code', 'course_name_mk'],
            ))
        )
    # Storing Stage
    if StageType.STORING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='store_curricula_data', stage_type=StageType.STORING)
            .add_step(PipelineStep(
                name='store_curricula_data',
                function=PipelineStep.save_data,
                output_file_name=Config.CURRICULA_OUTPUT_FILE_NAME,
                column_order=Config.CURRICULA_COLUMN_ORDER,
            ))
        )
    return builder.build()
