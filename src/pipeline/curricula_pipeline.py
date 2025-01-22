from src.pipeline.models.enums import DatasetType, StageType
from src.pipeline.common_steps import clean_course_code_step, clean_course_name_mk_step, clean_study_program_name_step
from src.patterns.mixin.file_storage import FileStorageMixin
from src.pipeline.models.pipeline import Pipeline
from src.patterns.builder.pipeline import PipelineBuilder
from src.patterns.builder.pipeline_stage import PipelineStageBuilder
from src.pipeline.models.step import PipelineStep
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
                input_file_location=FileStorageMixin.get_input_file_location(),
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
    # Storing Stage
    if StageType.STORING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='store_curricula_data', stage_type=StageType.STORING)
            .add_step(PipelineStep(
                name='store_curricula_data',
                function=PipelineStep.save_data,
                output_file_location=FileStorageMixin.get_output_file_location(),
                output_file_name=Config.CURRICULA_OUTPUT_FILE_NAME,
                column_order=Config.CURRICULA_COLUMN_ORDER,
            ))
        )
    return builder.build()
