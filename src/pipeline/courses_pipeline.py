from src.pipeline.models.enums import DatasetType, StageType
from src.pipeline.models.mixin import FileStorageMixin
from src.pipeline.models.pipeline import Pipeline
from src.patterns.builder.pipeline_builder import PipelineBuilder
from src.patterns.builder.pipeline_stage_builder import PipelineStageBuilder
from src.pipeline.models.step import PipelineStep
from src.field_parsers.clean_fields import clean_and_format_multivalued_field, clean_and_format_field, clean_professor_titles
from src.field_parsers.extract_fields import extract_course_level, extract_course_semester, extract_course_prerequisite_type, \
    extract_minimum_number_of_courses_passed
from src.field_parsers.transform_fields import transform_course_prerequisites
from src.pipeline.common_steps import clean_course_code_step, clean_course_name_mk_step
from src.config import Config


def build_courses_pipeline(stages: list[StageType] = tuple(StageType)) -> Pipeline:
    builder: PipelineBuilder = PipelineBuilder(name='courses_pipeline', dataset_type=DatasetType.COURSES)
    # Loading Stage
    if StageType.LOADING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='load_courses_data', stage_type=StageType.LOADING)
            .add_step(
                PipelineStep(
                    name='load_courses_data',
                    function=PipelineStep.read_data,
                    input_file_location=FileStorageMixin.get_input_file_location(),
                    input_file_name=Config.COURSES_INPUT_DATA_FILE_PATH,
                )
            )
        )
    # Cleaning Stage
    if StageType.CLEANING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='clean_courses_data', stage_type=StageType.CLEANING)
            .add_step(clean_course_code_step)
            .add_step(clean_course_name_mk_step)
            .add_step(
                PipelineStep(
                    name='clean_course_name_en',
                    function=PipelineStep.apply_function,
                    mapping_function=clean_and_format_field,
                    source_columns='course_name_en',
                    destination_columns='course_name_en',
                )
            )
            .add_step(
                PipelineStep(
                    name='clean_course_prerequisites',
                    function=PipelineStep.apply_function,
                    mapping_function=clean_and_format_multivalued_field,
                    source_columns='course_prerequisites',
                    destination_columns='course_prerequisites',
                )
            )
            .add_step(
                PipelineStep(
                    name='clean_course_professors_titles',
                    function=PipelineStep.apply_function,
                    mapping_function=clean_professor_titles,
                    source_columns='course_professors',
                    destination_columns='course_professors',
                )
            )
        )
    # Extraction Stage
    if StageType.EXTRACTING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='extract_courses_data', stage_type=StageType.EXTRACTING)
            .add_step(
                PipelineStep(
                    name='extract_course_level',
                    function=PipelineStep.apply_function,
                    mapping_function=extract_course_level,
                    source_columns='course_code',
                    destination_columns='course_level',
                )
            )
            .add_step(
                PipelineStep(
                    name='extract_course_semester',
                    function=PipelineStep.apply_function,
                    mapping_function=extract_course_semester,
                    source_columns=['course_academic_year', 'course_semester_season'],
                    destination_columns='course_semester',
                )
            )
            .add_step(
                PipelineStep(
                    name='extract_course_prerequisite_type',
                    function=PipelineStep.apply_function,
                    mapping_function=extract_course_prerequisite_type,
                    source_columns='course_prerequisites',
                    destination_columns='course_prerequisite_type',
                )
            )
            .add_step(
                PipelineStep(
                    name='extract_course_minimum_number_of_courses_passed',
                    function=PipelineStep.apply_function,
                    mapping_function=extract_minimum_number_of_courses_passed,
                    source_columns=['course_prerequisite_type', 'course_prerequisites'],
                    destination_columns='course_prerequisites_minimum_required_courses',
                )
            )
        )
    # Transformation Stage
    if StageType.TRANSFORMING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='transform_courses_data', stage_type=StageType.TRANSFORMING)
            .add_step(
                PipelineStep(
                    name='transform_course_prerequisites',
                    function=PipelineStep.apply_matching,
                    mapping_function=transform_course_prerequisites,
                    source_columns=['course_prerequisite_type', 'course_prerequisites', 'course_name_mk'],
                    destination_columns='course_prerequisites',
                    truth_columns='course_name_mk',
                )
            )
        )
    # Flattening Stage
    if StageType.FLATTENING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='flatten_courses_data', stage_type=StageType.FLATTENING)
            .add_step(
                PipelineStep(
                    name='flatten_course_prerequisites',
                    function=PipelineStep.explode_column,
                    source_columns='course_prerequisites',
                    destination_columns='course_prerequisites',
                )
            )
            .add_step(
                PipelineStep(
                    name='flatten_course_professors',
                    function=PipelineStep.explode_column,
                    source_columns='course_professors',
                    destination_columns='course_professors',
                )
            )
        )
    # Generation Stage
    if StageType.GENERATING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='generate_courses_data', stage_type=StageType.GENERATING)
            .add_step(
                PipelineStep(
                    name='generate_course_id',
                    function=PipelineStep.generate_indeces,
                    source_columns='course_code',
                    destination_columns='course_id',
                )
            )
            .add_step(
                PipelineStep(
                    name='generate_course_professors_id',
                    function=PipelineStep.generate_indeces,
                    source_columns='course_professors',
                    destination_columns='course_professors_id',
                )
            )
            .add_step(
                PipelineStep(
                    name='map_course_prerequisites',
                    function=PipelineStep.map_values_to_indeces,
                    value_column='course_prerequisites',
                    reference_column='course_id',
                    merge_column='course_name_mk',
                    destination_column='course_prerequisites_course_id',
                    how='left',
                )
            )
        )
    # Storing Stage
    if StageType.STORING in stages:
        builder.add_stage(
            PipelineStageBuilder(name='store_courses_data', stage_type=StageType.STORING)
            .add_step(
                PipelineStep(
                    name='store_courses_data',
                    function=PipelineStep.save_data,
                    output_file_location=FileStorageMixin.get_output_file_location(),
                    output_file_name=Config.COURSES_OUTPUT_FILE_NAME,
                    column_order=Config.COURSES_COLUMN_ORDER,
                )
            )
        )
    return builder.build()