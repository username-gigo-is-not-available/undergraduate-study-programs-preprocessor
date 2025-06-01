from src.patterns.builder.step import PipelineStep
from src.patterns.strategy.sanitization import RemoveExtraDelimitersTransformationStrategy, \
    CapitalizeSentenceTransformationStrategy

clean_study_program_name_step: PipelineStep = PipelineStep(
    name='clean-study-program-name',
    function=PipelineStep.apply,
    strategy=RemoveExtraDelimitersTransformationStrategy('study_program_name', ' ')
)

clean_course_code_step: PipelineStep = PipelineStep(
    name='clean-course-code',
    function=PipelineStep.apply,
    strategy=RemoveExtraDelimitersTransformationStrategy('course_code', ' ')
)

clean_course_name_mk_step: PipelineStep = PipelineStep(
    name='clean-course-name-mk',
    function=PipelineStep.apply,
    strategy=RemoveExtraDelimitersTransformationStrategy('course_name_mk', ' ')
    .then(CapitalizeSentenceTransformationStrategy('course_name_mk'))
)
