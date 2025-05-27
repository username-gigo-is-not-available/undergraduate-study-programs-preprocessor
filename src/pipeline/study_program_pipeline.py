from src.pipeline.models.enums import  StageType
from src.pipeline.common_steps import clean_study_program_name_step
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep
from src.field_parsers.extract_fields import extract_study_program_code
from src.config import Config


def study_programs_pipeline() -> Pipeline:
    return (Pipeline(name='study-programs-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOADING)
        .add_step(
            PipelineStep(
                name='load-study-program-data',
                function=PipelineStep.read_data,
                input_file_location=PipelineStep.get_input_file_location(),
                input_file_name=Config.STUDY_PROGRAMS_INPUT_DATA_FILE_PATH,
                column_order=Config.STUDY_PROGRAMS_INPUT_COLUMN_ORDER,
            )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEANING)
        .add_step(clean_study_program_name_step)
    )
    .add_stage(
        PipelineStage(name='extract-data', stage_type=StageType.EXTRACTING)
        .add_step(
            PipelineStep(
                name='extract-study-program-code',
                function=PipelineStep.apply,
                mapping_function=extract_study_program_code,
                input_columns=['study_program_url', 'study_program_duration'],
                output_columns='study_program_code'
            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATING)
        .add_step(
            PipelineStep(
                name='generate-study-program-id',
                function=PipelineStep.uuid,
                input_columns=['study_program_name', 'study_program_duration'],
                output_columns='study_program_id'
            )
        )
    )
    .add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORING)
        .add_step(
            PipelineStep(
                name='store-study-program-data',
                function=PipelineStep.save_data,
                output_file_location=PipelineStep.get_output_file_location(),
                output_file_name=Config.STUDY_PROGRAMS_OUTPUT_FILE_NAME,
                column_order=Config.STUDY_PROGRAMS_OUTPUT_COLUMN_ORDER
            )
        )
    )
    )
