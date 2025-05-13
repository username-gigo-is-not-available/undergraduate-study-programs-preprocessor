from src.patterns.builder.step import PipelineStep
from src.field_parsers.clean_fields import clean_field, clean_and_format_field

clean_study_program_name_step: PipelineStep = PipelineStep(
    name='clean-study-program-name',
    function=PipelineStep.apply,
    mapping_function=clean_field,
    source_columns='study_program_name',
    destination_columns='study_program_name',
)

clean_course_code_step: PipelineStep = PipelineStep(
    name='clean-course-code',
    function=PipelineStep.apply,
    mapping_function=clean_field,
    source_columns='course_code',
    destination_columns='course_code',
)

clean_course_name_mk_step: PipelineStep = PipelineStep(
    name='clean-course-name-mk',
    function=PipelineStep.apply,
    mapping_function=clean_and_format_field,
    source_columns='course_name_mk',
    destination_columns='course_name_mk',
)
