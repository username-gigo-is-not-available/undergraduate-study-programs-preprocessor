import pandas as pd

from src.config import Config
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep
from src.patterns.strategy.extraction import ProfessorNameStrategy, ProfessorSurnameStrategy
from src.patterns.strategy.sanitization import ReplaceValuesStrategy
from src.pipeline.common_steps import clean_course_code_step
from src.pipeline.models.enums import StageType


def professor_teaches_pipeline(df_courses: pd.DataFrame) -> Pipeline:
    return (Pipeline(name='professor-teaches-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOADING)
        .add_step(
            PipelineStep(
                name='load-professor-teaches-data',
                function=PipelineStep.read_data,
                input_file_location=PipelineStep.get_input_file_location(),
                input_file_name=Config.PROFESSORS_INPUT_DATA_FILE_PATH,
                columns=Config.PROFESSORS_INPUT_COLUMNS,
                drop_duplicates=True,
            )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEANING)
        .add_step(clean_course_code_step)
        .add_step(
            PipelineStep(
                name='clean-course-professors',
                function=PipelineStep.apply,
                strategy=ReplaceValuesStrategy('course_professors', Config.PROFESSOR_TITLES, '')
                .then(ReplaceValuesStrategy('course_professors', '\n', '|'))
            )
        )
    )
    .add_stage(
        PipelineStage(name='transform-data', stage_type=StageType.TRANSFORMING)
        .add_step(
            PipelineStep(
                name='flatten-course-professors',
                function=PipelineStep.explode,
                input_columns='course_professors',
                output_columns='course_professors',
                delimiter="|",
                drop_duplicates=True,
            )
        )
    )
    .add_stage(
        PipelineStage(name='extract-data', stage_type=StageType.EXTRACTING)
        .add_step(
            PipelineStep(
                name='extract-professor-name',
                function=PipelineStep.apply,
                strategy=ProfessorNameStrategy('course_professors', 'professor_name'),
            )
        )
        .add_step(
            PipelineStep(
                name='extract-professor-surname',
                function=PipelineStep.apply,
                strategy=ProfessorSurnameStrategy('course_professors', 'professor_surname'),
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
                on=['course_code'],
                how='inner'
            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATING)
        .add_step(
            PipelineStep(
                name='generate-professors-id',
                function=PipelineStep.uuid,
                input_columns=['professor_name', 'professor_surname'],
                output_columns='professor_id',
            )
        )
        .add_step(
            PipelineStep(
                name='generate-teaches-id',
                function=PipelineStep.uuid,
                input_columns=['course_id', 'professor_id'],
                output_columns='teaches_id',
            )
        )
    )
    .add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORING)
        .add_step(
            PipelineStep(
                name='store-professor-data',
                function=PipelineStep.save_data,
                output_file_location=PipelineStep.get_output_file_location(),
                output_file_name=Config.PROFESSORS_OUTPUT_FILE_NAME,
                columns=Config.PROFESSORS_OUTPUT_COLUMNS,
                drop_duplicates=True,
                drop_na=True
            )
        )
        .add_step(
            PipelineStep(
                name='store-teaches-data',
                function=PipelineStep.save_data,
                output_file_location=PipelineStep.get_output_file_location(),
                output_file_name=Config.TEACHES_OUTPUT_FILE_NAME,
                columns=Config.TEACHES_OUTPUT_COLUMNS,
                drop_duplicates=True
            )
        )
    ))
