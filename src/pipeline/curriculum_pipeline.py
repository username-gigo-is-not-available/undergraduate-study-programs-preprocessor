import pandas as pd

from src.configurations import DatasetConfiguration
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep
from src.patterns.strategy.extraction import CourseSemesterSeasonStrategy, CourseAcademicYearStrategy
from src.patterns.strategy.filtering import OrFilteringStrategy, NotEqualFilteringStrategy, NotNullFilteringStrategy, \
    GroupExistsFilteringStrategy, GroupHasAtLeastNMembersFilteringStrategy
from src.pipeline.common_steps import clean_study_program_name_step, clean_course_code_step, clean_course_name_mk_step
from src.pipeline.models.enums import StageType, CoursePrerequisiteType


def curriculum_pipeline(df_study_programs: pd.DataFrame, df_courses: pd.DataFrame,
                        df_requisites: pd.DataFrame) -> Pipeline:
    def merge_with_requisites_stage(df_requisites: pd.DataFrame) -> PipelineStage:
        return (
        PipelineStage(
            name='merge-data',
            stage_type=StageType.MERGE
        ).add_step(
            PipelineStep(
                name='merge-with-requisites-data',
                function=PipelineStep.merge,
                merge_df=df_requisites,
                on='course_id',
                how='left'
            )
        )
        .add_step(
            PipelineStep(
                name='look-up-prerequisite-study-program-id',
                function=PipelineStep.look_up,
                left_on=['study_program_id', 'prerequisite_course_id'],
                right_on=['study_program_id', 'course_id'],
                columns=['study_program_id', 'course_id'],
                how='left',
                prefix='prerequisite_'
            )
        )
    )
    def filter_invalid_curricula_stage() -> PipelineStage:
        return (
            PipelineStage(
                name='filter-data',
                stage_type=StageType.FILTER
            ).add_step(
                PipelineStep(
                    name='filter-one-type',
                    function=PipelineStep.filter,
                    strategy=OrFilteringStrategy(
                        left=NotEqualFilteringStrategy(
                            column='course_prerequisite_type',
                            value=CoursePrerequisiteType.ONE
                        ),
                        right=NotNullFilteringStrategy(column='prerequisite_study_program_id')
                    )
                )
            ).add_step(
                PipelineStep(
                    name='filter-any-type',
                    function=PipelineStep.filter,
                    strategy=OrFilteringStrategy(
                        left=NotEqualFilteringStrategy(
                            column='course_prerequisite_type',
                            value=CoursePrerequisiteType.ANY
                        ),
                        right=GroupExistsFilteringStrategy(
                            group_by_columns=['study_program_id', 'course_id'],
                            evaluated_columns='prerequisite_study_program_id'
                        )
                    )
                )
            ).add_step(
                PipelineStep(
                    name='filter-total-type',
                    function=PipelineStep.filter,
                    strategy=OrFilteringStrategy(
                        left=NotEqualFilteringStrategy(
                            column='course_prerequisite_type',
                            value=CoursePrerequisiteType.TOTAL
                        ),
                        right=GroupHasAtLeastNMembersFilteringStrategy(
                            group_by_columns=['study_program_id', 'course_id'],
                            evaluated_columns='prerequisite_study_program_id',
                            threshold_columns='minimum_required_number_of_courses'
                        )
                    )
                )
            )
    )

    pipeline: Pipeline =  (Pipeline(name='curriculum-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOAD)
        .add_step(
            PipelineStep(
                name='load-curriculum-data',
                function=PipelineStep.read_data,
                configuration=DatasetConfiguration.CURRICULA
            )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEAN)
        .add_step(clean_study_program_name_step)
        .add_step(clean_course_code_step)
        .add_step(clean_course_name_mk_step)
    )
    .add_stage(
        PipelineStage(name='merge-data', stage_type=StageType.MERGE)
        .add_step(
            PipelineStep(
                name='merge-with-course-data',
                function=PipelineStep.merge,
                merge_df=df_courses,
                on=['course_code', 'course_name_mk'],
                how='inner'
            )
        )
        .add_step(
            PipelineStep(
                name='merge-with-study-program-data',
                function=PipelineStep.merge,
                merge_df=df_study_programs,
                on=['study_program_name', 'study_program_duration'],
                how='inner'
            )
        )
    )
    .add_stage(
        PipelineStage(name='extract-data', stage_type=StageType.EXTRACT)
        .add_step(
            PipelineStep(
                name='extract-course-semester-season',
                function=PipelineStep.apply,
                strategy=CourseSemesterSeasonStrategy('course_semester', 'course_semester_season')
            )
        )
        .add_step(
            PipelineStep(
                name='extract-course-academic-year',
                function=PipelineStep.apply,
                strategy=CourseAcademicYearStrategy('course_semester', 'course_academic_year')
            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATE)
        .add_step(
            PipelineStep(
                name='generate-curriculum-id',
                function=PipelineStep.uuid,
                input_columns=['study_program_id', 'course_id', 'course_type', 'course_semester',
                               'course_academic_year', 'course_semester_season'],
                output_columns='curriculum_id',
            )
        )
        .add_step(
            PipelineStep(
                name='generate-offers-id',
                function=PipelineStep.uuid,
                input_columns=['curriculum_id', 'study_program_id'],
                output_columns='offers_id',
            )
        )
        .add_step(
            PipelineStep(
                name='generate-includes-id',
                function=PipelineStep.uuid,
                input_columns=['curriculum_id', 'course_id'],
                output_columns='includes_id',
            )
        )
    )
    ).build()

    pipeline.add_stage(merge_with_requisites_stage(df_requisites))
    pipeline.add_stage(filter_invalid_curricula_stage())
    pipeline.add_stage(
        PipelineStage(
            name='select-data',
            stage_type=StageType.SELECT
        )
        .add_step(
            PipelineStep(
                name='select-curricula-data',
                function=PipelineStep.select,
                columns=DatasetConfiguration.CURRICULA.output_transformation_config.columns + ['course_id', 'study_program_id'],
                drop_duplicates=True
            )
        )
    )
    pipeline.add_stage(merge_with_requisites_stage(df_requisites))
    pipeline.add_stage(filter_invalid_curricula_stage())
    pipeline.add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORE)
        .add_step(
            PipelineStep(
                name='store-curricula-data',
                function=PipelineStep.save_data,
                configuration=DatasetConfiguration.CURRICULA
            )
        )
        .add_step(
            PipelineStep(
                name='store-offers-data',
                function=PipelineStep.save_data,
                configuration=DatasetConfiguration.OFFERS
            )
        )
        .add_step(
            PipelineStep(
                name='store-includes-data',
                function=PipelineStep.save_data,
                configuration=DatasetConfiguration.INCLUDES
            )
        )
    )
    return pipeline.build()
