from src.pipeline.models.enums import DatasetType, StageType
from src.pipeline.common_steps import clean_study_program_name_step
from src.pipeline.models.mixin import FileStorageMixin
from src.pipeline.models.pipeline import Pipeline
from src.patterns.builder.pipeline_stage_builder import PipelineStageBuilder
from src.pipeline.models.step import PipelineStep
from src.field_parsers.extract_fields import extract_study_program_code
from src.patterns.builder.pipeline_builder import PipelineBuilder
from src.config import Config


def build_study_programs_pipeline(stages: list[StageType] = tuple(StageType)) -> Pipeline:
    builder: PipelineBuilder = PipelineBuilder(name='study_programs_pipeline', dataset_type=DatasetType.STUDY_PROGRAMS)
    # Loading Stage
    if StageType.LOADING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='load_study_programs_data', stage_type=StageType.LOADING)
            .add_step(
                PipelineStep(
                    name='load_study_programs_data',
                    function=PipelineStep.read_data,
                    input_file_location=FileStorageMixin.get_input_file_location(),
                    input_file_name=Config.STUDY_PROGRAMS_INPUT_DATA_FILE_PATH,
                )
            )
        )
    # Cleaning Stage
    if StageType.CLEANING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='clean_study_programs_data', stage_type=StageType.CLEANING)
            .add_step(clean_study_program_name_step)
        )
    # Extraction Stage
    if StageType.EXTRACTING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='extract_study_programs_data', stage_type=StageType.EXTRACTING)
            .add_step(
                PipelineStep(
                    name='extract_study_program_code',
                    function=PipelineStep.apply_function,
                    mapping_function=extract_study_program_code,
                    source_columns=['study_program_url', 'study_program_duration'],
                    destination_columns='study_program_code'
                )
            )
        )
    # Generation Stage
    if StageType.GENERATING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='generate_study_programs_data', stage_type=StageType.GENERATING)
            .add_step(
                PipelineStep(
                    name='generate_study_program_id',
                    function=PipelineStep.generate_indeces,
                    source_columns='study_program_code',
                    destination_columns='study_program_id'
                )
            )
        )
    # Storing Stage
    if StageType.STORING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='store_study_programs_data', stage_type=StageType.STORING)
            .add_step(
                PipelineStep(
                    name='store_study_programs_data',
                    function=PipelineStep.save_data,
                    output_file_location=FileStorageMixin.get_output_file_location(),
                    output_file_name=Config.STUDY_PROGRAMS_OUTPUT_FILE_NAME,
                    column_order=Config.STUDY_PROGRAMS_COLUMN_ORDER
                )
            )
        )
    return builder.build()

