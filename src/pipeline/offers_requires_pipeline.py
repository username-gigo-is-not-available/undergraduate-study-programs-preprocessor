import pandas as pd

from src.config import Config
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep
from src.patterns.strategy.extraction import CourseLevelStrategy, CourseSemesterSeasonStrategy, \
    CourseAcademicYearStrategy, CoursePrerequisiteTypeStrategy, MinimumNumberOfCoursesStrategy
from src.patterns.strategy.sanitization import RemoveExtraDelimitersStrategy, \
    ReplaceValuesStrategy
from src.patterns.strategy.transformation import CoursePrerequisiteStrategy
from src.pipeline.common_steps import clean_course_code_step, clean_course_name_mk_step, clean_study_program_name_step
from src.pipeline.models.enums import StageType


def offers_requires_pipeline(df_study_programs: pd.DataFrame, df_courses: pd.DataFrame) -> Pipeline:
    return (Pipeline(name='curriculum-prerequisites-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOADING)
        .add_step(PipelineStep(
            name='load-curriculum-prerequisites-data',
            function=PipelineStep.read_data,
            input_file_location=PipelineStep.get_input_file_location(),
            input_file_name=Config.CURRICULA_INPUT_DATA_FILE_PATH,
            column_order=Config.OFFERS_REQUIRES_COLUMN_ORDER
        )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEANING)
        .add_step(clean_study_program_name_step)
        .add_step(clean_course_code_step)
        .add_step(clean_course_name_mk_step)
        .add_step(
            PipelineStep(
                name='clean-course-prerequisites',
                function=PipelineStep.apply,
                strategy=RemoveExtraDelimitersStrategy('course_prerequisites', ' ')
                .then(RemoveExtraDelimitersStrategy('course_prerequisites', '\n')
                      .then(ReplaceValuesStrategy('course_prerequisites', ' или ', '|')
                            )
                      )
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
                on=['course_code', 'course_name_mk'],
                how='inner'
            ))
        .add_step(
            PipelineStep(
                name='merge-with-study-program-data',
                function=PipelineStep.merge,
                merge_df=df_study_programs,
                on=['study_program_name', 'study_program_duration'],
                how='inner'
            ))
    )
    .add_stage(
        PipelineStage(name='extract-data', stage_type=StageType.EXTRACTING)
        .add_step(
            PipelineStep(
                name='extract-course-level',
                function=PipelineStep.apply,
                strategy=CourseLevelStrategy('course_code', 'course_level')
            )
        )
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
        .add_step(
            PipelineStep(
                name='extract-course-prerequisite-type',
                function=PipelineStep.apply,
                strategy=CoursePrerequisiteTypeStrategy('course_prerequisites', 'course_prerequisite_type')
            )
        )
        .add_step(
            PipelineStep(
                name='extract-minimum-required-number-of-courses',
                function=PipelineStep.apply,
                strategy=MinimumNumberOfCoursesStrategy('course_prerequisites', 'course_prerequisite_type',
                                                        'minimum_required_number_of_courses')
            )
        )
    )
    .add_stage(
        PipelineStage(name='transform-data', stage_type=StageType.TRANSFORMING)
        .add_step(
            PipelineStep(
                name='transform-course-prerequisites',
                function=PipelineStep.apply,
                strategy=CoursePrerequisiteStrategy('course_prerequisites', 'course_name_mk',
                                                    'course_prerequisite_type', "|")
            )
        )
    )
    .add_stage(
        PipelineStage(name='flatten-data', stage_type=StageType.FLATTENING)
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
        .add_step(
            PipelineStep(
                name='link-course-prerequisite-ids',
                function=PipelineStep.link,
                value_column='course_prerequisites',
                reference_column='course_id',
                merge_column='course_name_mk',
                output_column='course_prerequisite_id',
                how='left',
            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATING)
        .add_step(
            PipelineStep(
                name='generate-offers-id',
                function=PipelineStep.uuid,
                input_columns=['study_program_id', 'course_id'],
                output_columns='offers_id',
            )
        )
        .add_step(
            PipelineStep(
                name='generate-requires-id',
                function=PipelineStep.uuid,
                input_columns=['course_id', 'course_prerequisite_id', 'course_prerequisite_type'],
                output_columns='requires_id',
            )
        )
    )
    .add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORING)
        .add_step(
            PipelineStep(
                name='store-curricula-data',
                function=PipelineStep.save_data,
                output_file_location=PipelineStep.get_output_file_location(),
                output_file_name=Config.OFFERS_OUTPUT_FILE_NAME,
                column_order=Config.OFFERS_OUTPUT_COLUMN_ORDER,
                drop_duplicates=True
            )
        )
        .add_step(
            PipelineStep(
                name='store-prerequisites-data',
                function=PipelineStep.save_data,
                output_file_location=PipelineStep.get_output_file_location(),
                output_file_name=Config.REQUIRES_OUTPUT_FILE_NAME,
                column_order=Config.REQUIRES_OUTPUT_COLUMN_ORDER,
                drop_duplicates=True
            ))
    ))
