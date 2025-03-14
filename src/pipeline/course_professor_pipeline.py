from src.config import Config
from src.field_parsers.clean_fields import clean_professor_titles, clean_and_format_field
from src.field_parsers.extract_fields import extract_professor_name, extract_professor_surname
from src.patterns.mixin.file_storage import FileStorageMixin
from src.pipeline.common_steps import clean_course_code_step, clean_course_name_mk_step
from src.pipeline.models.enums import StageType
from src.patterns.builder.pipeline import Pipeline
from src.patterns.builder.stage import PipelineStage
from src.patterns.builder.step import PipelineStep


def build_course_professor_pipeline():
    return (Pipeline(name='course-professor-pipeline')
    .add_stage(
        PipelineStage(name='load-data', stage_type=StageType.LOADING)
        .add_step(
            PipelineStep(
                name='load-course-professor-data',
                function=PipelineStep.read_data,
                input_file_location=FileStorageMixin.get_input_file_location(),
                input_file_name=Config.COURSES_INPUT_DATA_FILE_PATH,
                column_order=Config.COURSE_PROFESSOR_INPUT_COLUMN_ORDER,
                drop_duplicates=True,
            )
        )
    )
    .add_stage(
        PipelineStage(name='clean-data', stage_type=StageType.CLEANING)
        .add_step(clean_course_code_step)
        .add_step(clean_course_name_mk_step)
        .add_step(
            PipelineStep(
                name='clean-course-name-en',
                function=PipelineStep.apply_function,
                mapping_function=clean_and_format_field,
                source_columns='course_name_en',
                destination_columns='course_name_en',
            )
        )
        .add_step(
            PipelineStep(
                name='clean-course-professor-titles',
                function=PipelineStep.apply_function,
                mapping_function=clean_professor_titles,
                source_columns='course_professors',
                destination_columns='course_professors',
            )
        )
    )
    .add_stage(
        PipelineStage(name='transform-data', stage_type=StageType.TRANSFORMING)
        .add_step(
            PipelineStep(
                name='flatten-course-professors',
                function=PipelineStep.explode_column,
                source_columns='course_professors',
                destination_columns='course_professors',
                drop_duplicates=True,
            )
        )
    )
    .add_stage(
        PipelineStage(name='extract-data', stage_type=StageType.EXTRACTING)
        .add_step(
            PipelineStep(
                name='extract-professor-name',
                function=PipelineStep.apply_function,
                mapping_function=extract_professor_name,
                source_columns='course_professors',
                destination_columns='professor_name',
            )
        )
        .add_step(
            PipelineStep(
                name='extract-professor-surname',
                function=PipelineStep.apply_function,
                mapping_function=extract_professor_surname,
                source_columns='course_professors',
                destination_columns='professor_surname',
            )
        )
    )
    .add_stage(
        PipelineStage(name='generate-data', stage_type=StageType.GENERATING)
        .add_step(
            PipelineStep(
                name='generate-professors-id',
                function=PipelineStep.generate_indeces,
                source_columns='course_professors',
                destination_columns='professor_id',
            )
        )
        .add_step(
            PipelineStep(
                name='generate-course-id',
                function=PipelineStep.generate_indeces,
                source_columns='course_code',
                destination_columns='course_id',
            )
        )
    )
    .add_stage(
        PipelineStage(name='store-data', stage_type=StageType.STORING)
        .add_step(
            PipelineStep(
                name='store-course-data',
                function=PipelineStep.save_data,
                output_file_location=FileStorageMixin.get_output_file_location(),
                output_file_name=Config.COURSES_OUTPUT_FILE_NAME,
                column_order=Config.COURSES_OUTPUT_COLUMN_ORDER,
                drop_duplicates=True,
            )
        )
        .add_step(
            PipelineStep(
                name='store-professor-data',
                function=PipelineStep.save_data,
                output_file_location=FileStorageMixin.get_output_file_location(),
                output_file_name=Config.PROFESSORS_OUTPUT_FILE_NAME,
                column_order=Config.PROFESSORS_OUTPUT_COLUMN_ORDER,
                drop_duplicates=True,
            )
        )
        .add_step(
            PipelineStep(
                name='store-course-professor-data',
                function=PipelineStep.save_data,
                output_file_location=FileStorageMixin.get_output_file_location(),
                output_file_name=Config.TAUGHT_BY_OUTPUT_FILE_NAME,
                column_order=Config.TAUGHT_BY_OUTPUT_COLUMN_ORDER,
                drop_duplicates=True
            )
        )
    )).build()
