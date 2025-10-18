from src.configurations import DatasetConfiguration
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep
from src.patterns.strategy.extraction import StudyProgramCodeExtractionStrategy
from src.pipeline.common_steps import clean_study_program_name_step
from src.pipeline.models.enums import StageType


def study_programs_pipeline() -> Pipeline:
    return (Pipeline(name='study-programs-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOAD)
        .add_step(
            PipelineStep(
                name='load-study-program-data',
                function=PipelineStep.read_data,
                configuration=DatasetConfiguration.STUDY_PROGRAMS
            )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEAN)
        .add_step(clean_study_program_name_step)
    )
    .add_stage(
        PipelineStage(name='extract-data', stage_type=StageType.EXTRACT)
        .add_step(
            PipelineStep(
                name='extract-study-program-code',
                function=PipelineStep.apply,
                strategy=StudyProgramCodeExtractionStrategy(url_column='study_program_url',
                                                            duration_column='study_program_duration',
                                                            output_column='study_program_code'),
            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATE)
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
        PipelineStage(name='store-data', stage_type=StageType.STORE)
        .add_step(
            PipelineStep(
                name='store-study-program-data',
                function=PipelineStep.save_data,
                configuration=DatasetConfiguration.STUDY_PROGRAMS
            )
        )
    )
    )
