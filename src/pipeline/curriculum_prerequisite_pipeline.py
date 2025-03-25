import pandas as pd

from src.field_parsers.clean_fields import clean_prerequisites
from src.field_parsers.extract_fields import extract_course_prerequisite_type, extract_minimum_number_of_courses_passed, \
    extract_course_level, extract_course_semester_season, extract_course_academic_year, update_course_prerequisite_type
from src.field_parsers.transform_fields import transform_course_prerequisites
from src.pipeline.models.enums import StageType, CoursePrerequisiteType
from src.pipeline.common_steps import clean_course_code_step, clean_course_name_mk_step, clean_study_program_name_step
from src.patterns.mixin.file_storage import FileStorageMixin
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep
from src.config import Config


def build_curriculum_prerequisites_pipeline() -> Pipeline:
    df_courses: pd.DataFrame = Pipeline(name='course-pipeline').add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOADING)
        .add_step(PipelineStep(
            name='load-courses-data',
            function=PipelineStep.read_data,
            input_file_location=FileStorageMixin.get_output_file_location(),
            input_file_name=Config.COURSES_OUTPUT_FILE_NAME,
        )
        )
    ).build().run()

    df_study_programs: pd.DataFrame = Pipeline(name='study-program-pipeline').add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOADING)
        .add_step(PipelineStep(
            name='load-study-programs-data',
            function=PipelineStep.read_data,
            input_file_location=FileStorageMixin.get_output_file_location(),
            input_file_name=Config.STUDY_PROGRAMS_OUTPUT_FILE_NAME,
        )
        )
    ).build().run()

    return (Pipeline(name='curriculum-prerequisites-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOADING)
        .add_step(PipelineStep(
            name='load-curriculum-prerequisites-data',
            function=PipelineStep.read_data,
            input_file_location=FileStorageMixin.get_input_file_location(),
            input_file_name=Config.CURRICULA_INPUT_DATA_FILE_PATH,
            column_order=Config.CURRICULUM_PREREQUISITES_INPUT_COLUMN_ORDER
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
                function=PipelineStep.apply_function,
                mapping_function=clean_prerequisites,
                source_columns='course_prerequisites',
                destination_columns='course_prerequisites',
            )
        )
    )
    .add_stage(
        PipelineStage(name='merge-data', stage_type=StageType.MERGING)
        .add_step(
            PipelineStep(
                name='merge-with-course-data',
                function=PipelineStep.merge_dataframes,
                merge_df=df_courses,
                on=['course_code', 'course_name_mk'],
                how='inner'
            ))
        .add_step(
            PipelineStep(
                name='merge-with-study-program-data',
                function=PipelineStep.merge_dataframes,
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
                function=PipelineStep.apply_function,
                mapping_function=extract_course_level,
                source_columns='course_code',
                destination_columns='course_level',
            )
        )
        .add_step(
            PipelineStep(
                name='extract-course-semester-season',
                function=PipelineStep.apply_function,
                mapping_function=extract_course_semester_season,
                source_columns='course_semester',
                destination_columns='course_semester_season',
            )
        )
        .add_step(
            PipelineStep(
                name='extract-course-academic-year',
                function=PipelineStep.apply_function,
                mapping_function=extract_course_academic_year,
                source_columns='course_semester',
                destination_columns='course_academic_year',
            )
        )
        .add_step(
            PipelineStep(
                name='extract-course-prerequisite-type',
                function=PipelineStep.apply_function,
                mapping_function=extract_course_prerequisite_type,
                source_columns='course_prerequisites',
                destination_columns='course_prerequisite_type',
            )
        )
        .add_step(
            PipelineStep(
                name='extract-minimum-required-number-of-courses',
                function=PipelineStep.apply_function,
                mapping_function=extract_minimum_number_of_courses_passed,
                source_columns=['course_prerequisite_type', 'course_prerequisites'],
                destination_columns='minimum_required_number_of_courses',
            )
        )
    )
    .add_stage(
        PipelineStage(name='transform-data', stage_type=StageType.TRANSFORMING)
        .add_step(
            PipelineStep(
                name='transform-course-prerequisites',
                function=PipelineStep.apply_matching,
                mapping_function=transform_course_prerequisites,
                source_columns=['course_prerequisite_type', 'course_prerequisites', 'course_name_mk'],
                destination_columns='course_prerequisites',
                group_by_columns=['study_program_name', 'study_program_duration'],
                truth_columns='course_name_mk',
                sort_columns='course_prerequisite_type',
                sort_order={
                    val: i for i, val in
                    enumerate(
                        [CoursePrerequisiteType.NONE, CoursePrerequisiteType.ONE, CoursePrerequisiteType.ANY, CoursePrerequisiteType.TOTAL]
                    )
                }
            )
        )
    )
    .add_stage(
        PipelineStage(name='update-data', stage_type=StageType.EXTRACTING)
        .add_step(
            PipelineStep(
                name='update-course-prerequisite-type',
                function=PipelineStep.apply_function,
                mapping_function=update_course_prerequisite_type,
                source_columns=['course_prerequisites', 'course_prerequisite_type'],
                destination_columns='course_prerequisite_type',
            )
        )
    )
    .add_stage(
        PipelineStage(name='flatten-data', stage_type=StageType.FLATTENING)
        .add_step(
            PipelineStep(
                name='flatten-course-prerequisites',
                function=PipelineStep.explode_column,
                source_columns='course_prerequisites',
                destination_columns='course_prerequisites',
            )
        )
        .add_step(
            PipelineStep(
                name='map-course-prerequisites',
                function=PipelineStep.map_values_to_indeces,
                value_column='course_prerequisites',
                reference_column='course_id',
                merge_column='course_name_mk',
                destination_column='course_prerequisite_id',
                how='left',
            )
        )
    )
    .add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORING)
        .add_step(PipelineStep(
            name='store-curricula-data',
            function=PipelineStep.save_data,
            output_file_location=FileStorageMixin.get_output_file_location(),
            output_file_name=Config.CURRICULA_OUTPUT_FILE_NAME,
            column_order=Config.CURRICULA_OUTPUT_COLUMN_ORDER,
            drop_duplicates=True
        )
        )
        .add_step(PipelineStep(
            name='store-prerequisites-data',
            function=PipelineStep.save_data,
            output_file_location=FileStorageMixin.get_output_file_location(),
            output_file_name=Config.PREREQUISITES_OUTPUT_FILE_NAME,
            column_order=Config.PREREQUISITES_OUTPUT_COLUMN_ORDER,
            drop_duplicates=True
        ))
    )).build()
