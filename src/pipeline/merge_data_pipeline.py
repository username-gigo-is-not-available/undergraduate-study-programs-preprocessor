import pandas as pd
from src.pipeline.models.enums import DatasetType, StageType
from src.pipeline.models.mixin import FileStorageMixin
from src.pipeline.models.pipeline import Pipeline
from src.patterns.builder.pipeline_builder import PipelineBuilder
from src.patterns.builder.pipeline_stage_builder import PipelineStageBuilder
from src.pipeline.models.step import PipelineStep
from src.config import Config


def build_merge_data_pipeline(df_study_programs: pd.DataFrame | None = None,
                              df_curricula: pd.DataFrame | None = None,
                              df_courses: pd.DataFrame | None = None) -> Pipeline:

    # Loading Stage
    if df_study_programs is None and df_curricula is None and df_courses is None:
        study_program_builder = PipelineBuilder(name='study_programs_pipeline', dataset_type=DatasetType.STUDY_PROGRAMS)
        curricula_builder = PipelineBuilder(name='curricula_pipeline', dataset_type=DatasetType.CURRICULA)
        courses_builder = PipelineBuilder(name='courses_pipeline', dataset_type=DatasetType.COURSES)
        study_program_builder.add_stage(
            PipelineStageBuilder(name='load_study_programs', stage_type=StageType.LOADING)
            .add_step(
                PipelineStep(
                    name='load_study_programs',
                    function=PipelineStep.read_data,
                    input_file_location=FileStorageMixin.get_output_file_location(),
                    input_file_name=Config.STUDY_PROGRAMS_OUTPUT_FILE_NAME,
                )
            )
        )
        curricula_builder.add_stage(
            PipelineStageBuilder(name='load_curricula', stage_type=StageType.LOADING)
            .add_step(
                PipelineStep(
                    name='load_curricula',
                    function=PipelineStep.read_data,
                    input_file_location=FileStorageMixin.get_output_file_location(),
                    input_file_name=Config.CURRICULA_OUTPUT_FILE_NAME,
                )
            )
        )
        courses_builder.add_stage(
            PipelineStageBuilder(name='load_courses', stage_type=StageType.LOADING)
            .add_step(
                PipelineStep(
                    name='load_courses',
                    function=PipelineStep.read_data,
                    input_file_location=FileStorageMixin.get_output_file_location(),
                    input_file_name=Config.COURSES_OUTPUT_FILE_NAME,
                )
            )
        )
        df_study_programs = study_program_builder.build().run()
        df_curricula = curricula_builder.build().run()
        df_courses = courses_builder.build().run()

    # Merging Stage
    builder: PipelineBuilder = PipelineBuilder(name='merge_pipeline', df=df_study_programs, dataset_type=DatasetType.MERGED_DATA)
    builder.add_stage(
        PipelineStageBuilder(name='merge_stage', stage_type=StageType.MERGING)
        .add_step(
            PipelineStep(
                name='merge_study_programs_and_curricula',
                function=PipelineStep.merge_dataframes,
                merge_df=df_curricula,
                on=['study_program_name', 'study_program_duration', 'study_program_url'],
                how='inner'
            )
        )
    )

    builder.add_stage(
        PipelineStageBuilder(name='merge_stage', stage_type=StageType.MERGING)
        .add_step(
            PipelineStep(
                name='merge_curricula_and_courses',
                function=PipelineStep.merge_dataframes,
                merge_df=df_courses,
                on=['course_code', 'course_name_mk', 'course_url'],
                how='inner'
            )
        )
    )
    # Storing Stage
    builder.add_stage(
        PipelineStageBuilder(name='storing_stage', stage_type=StageType.STORING)
        .add_step(
            PipelineStep(
                name='store_data',
                function=PipelineStep.save_data,
                output_file_location=FileStorageMixin.get_output_file_location(),
                output_file_name=Config.MERGED_DATA_OUTPUT_FILE_NAME,
                column_order=Config.MERGED_DATA_COLUMN_ORDER
            )
        )
    )
    return builder.build()
