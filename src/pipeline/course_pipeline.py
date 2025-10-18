from src.configurations import DatasetConfiguration
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep
from src.patterns.strategy.extraction import CourseLevelExtractionStrategy, CourseAbbreviationExtractionStrategy
from src.patterns.strategy.sanitization import RemoveExtraDelimitersStrategy, \
    PreserveAcronymsSentenceCaseStrategy
from src.pipeline.common_steps import clean_course_code_step, clean_course_name_mk_step
from src.pipeline.models.enums import StageType


def course_pipeline() -> Pipeline:
    return (Pipeline(name='course-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOAD)
        .add_step(
            PipelineStep(
                name='load-course-data',
                function=PipelineStep.read_data,
                configuration=DatasetConfiguration.COURSES,
            )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEAN)
        .add_step(clean_course_code_step)
        .add_step(clean_course_name_mk_step)
        .add_step(
            PipelineStep(
                name='clean-course-name-en',
                function=PipelineStep.apply,
                strategy=RemoveExtraDelimitersStrategy(column='course_name_en', delimiter=' ')
                .then(PreserveAcronymsSentenceCaseStrategy(column='course_name_en'))
            )
        )
    )
    .add_stage(
        PipelineStage(name='extract-data', stage_type=StageType.EXTRACT)
        .add_step(
            PipelineStep(
                name='extract-course-level',
                function=PipelineStep.apply,
                strategy=CourseLevelExtractionStrategy(code_column='course_code', output_column='course_level')
            )
        )
        .add_step(
            PipelineStep(
                name='extract-course-abbreviation',
                function=PipelineStep.apply,
                strategy=CourseAbbreviationExtractionStrategy(course_name_mk_column='course_name_mk',
                                                              output_column='course_abbreviation')
            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATE)
        .add_step(
            PipelineStep(
                name='generate-course-id',
                function=PipelineStep.uuid,
                input_columns='course_name_mk',
                output_columns='course_id',
            )
        )
    )
    .add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORE)
        .add_step(
            PipelineStep(
                name='store-course-data',
                function=PipelineStep.save_data,
                configuration=DatasetConfiguration.COURSES,
            )
        )
    )
    )
