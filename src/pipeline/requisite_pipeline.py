import pandas as pd

from src.configurations import DatasetConfiguration
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep
from src.patterns.strategy.extraction import CoursePrerequisiteTypeExtractionStrategy, \
    MinimumNumberOfCoursesExtractionStrategy
from src.patterns.strategy.sanitization import RemoveExtraDelimitersStrategy, \
    ReplaceValuesStrategy
from src.patterns.strategy.matching import CoursePrerequisiteMatchingStrategy
from src.pipeline.common_steps import clean_course_name_mk_step
from src.pipeline.models.enums import StageType


def requisite_pipeline(df_courses: pd.DataFrame) -> Pipeline:
    return (Pipeline(name='requisites-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOAD)
        .add_step(PipelineStep(
            name='load-course-data',
            function=PipelineStep.read_data,
            configuration=DatasetConfiguration.REQUISITES
        )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEAN)
        .add_step(clean_course_name_mk_step)
        .add_step(
            PipelineStep(
                name='clean-course-prerequisites',
                function=PipelineStep.apply,
                strategy=RemoveExtraDelimitersStrategy(column='course_prerequisites', delimiter=' ')
                .then(RemoveExtraDelimitersStrategy(column='course_prerequisites', delimiter='\n')
                      .then(ReplaceValuesStrategy(column='course_prerequisites', values=' или ', replacement='|')
                            )
                      )
            )
        )
    )
    .add_stage(
        PipelineStage(name='extract-data', stage_type=StageType.EXTRACT)
        .add_step(
            PipelineStep(
                name='extract-course-prerequisite-type',
                function=PipelineStep.apply,
                strategy=CoursePrerequisiteTypeExtractionStrategy(prerequisite_column='course_prerequisites',
                                                                  output_column='course_prerequisite_type')
            )
        )
        .add_step(
            PipelineStep(
                name='extract-minimum-required-number-of-courses',
                function=PipelineStep.apply,
                strategy=MinimumNumberOfCoursesExtractionStrategy(prerequisite_column='course_prerequisites',
                                                                  prerequisite_type_column='course_prerequisite_type',
                                                                  output_column='minimum_required_number_of_courses')
            )
        )
    )
    .add_stage(
        PipelineStage(name='match-data', stage_type=StageType.MATCH)
        .add_step(
            PipelineStep(
                name='match-course-prerequisites',
                function=PipelineStep.apply,
                strategy=CoursePrerequisiteMatchingStrategy(column='course_prerequisites',
                                                            course_name_mk_column='course_name_mk',
                                                            prerequisite_type_column='course_prerequisite_type',
                                                            values=[row for row in df_courses[
                                                                'course_name_mk'].drop_duplicates().values.tolist()],
                                                            delimiter="|")
            )
        )
    )
    .add_stage(
        PipelineStage(name='flatten-data', stage_type=StageType.FLATTEN)
        .add_step(
            PipelineStep(
                name='flatten-course-prerequisites',
                function=PipelineStep.explode,
                input_columns='course_prerequisites',
                output_columns='course_prerequisites',
                delimiter="|",
                drop_duplicates=True,
            )
        )
    )
    .add_stage(
        PipelineStage(name='merge-data', stage_type=StageType.MERGE)
        .add_step(
            PipelineStep(
                name='look-up-course-id',
                function=PipelineStep.merge,
                on='course_name_mk',
                merge_df=df_courses
            )
        )
        .add_step(
            PipelineStep(
                name='look-up-course-prerequisite-id',
                function=PipelineStep.self_merge,
                left_on='course_prerequisites',
                right_on='course_name_mk',
                columns=['course_name_mk', 'course_id'],
                prefix='prerequisite_'

            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATE)
        .add_step(
            PipelineStep(
                name='generate-requisite-id',
                function=PipelineStep.uuid,
                input_columns=['course_id', 'prerequisite_course_id', 'course_prerequisite_type'],
                output_columns='requisite_id',
            )
        )
        .add_step(
            PipelineStep(
                name='generate-requires-id',
                function=PipelineStep.uuid,
                input_columns=['requisite_id', 'course_id'],
                output_columns='requires_id',
            )
        )
        .add_step(
            PipelineStep(
                name='generate-satisfies-id',
                function=PipelineStep.uuid,
                input_columns=['requisite_id', 'prerequisite_course_id'],
                output_columns='satisfies_id',
            )
        )
    )
    .add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORE)
        .add_step(
            PipelineStep(
                name='store-requisites-data',
                function=PipelineStep.save_data,
                configuration=DatasetConfiguration.REQUISITES
            ))
        .add_step(
            PipelineStep(
                name='store-requires-data',
                function=PipelineStep.save_data,
                configuration=DatasetConfiguration.REQUIRES
            )
        )
        .add_step(
            PipelineStep(
                name='store-satisfies-data',
                function=PipelineStep.save_data,
                configuration=DatasetConfiguration.SATISFIES
            )
        )
    ))
