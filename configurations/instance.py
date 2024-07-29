from pathlib import Path

from configurations.model import Configuration
from settings import ENVIRONMENT_VARIABLES
from static import OUTPUT_DIRECTORY_PATH
from pipeline.instances.study_programs_pipeline import run_study_programs_pipeline
from pipeline.instances.curricula_pipeline import run_curricula_pipeline
from pipeline.instances.course_pipeline import run_courses_pipeline

study_programs_config = Configuration(configuration_name='study_programs_configuration',
                                      dataset_name='study_programs',
                                      input_file_path=Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_INPUT_DATA_FILE_PATH')),
                                      output_file_name=Path(ENVIRONMENT_VARIABLES.get('STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME')),
                                      output_directory_path=OUTPUT_DIRECTORY_PATH,
                                      order_by=['study_program_id'],
                                      columns_order=['study_program_id',
                                                     'study_program_name',
                                                     'study_program_duration',
                                                     'study_program_url'],
                                      pipeline=run_study_programs_pipeline,
                                      )

curricula_config = Configuration(configuration_name='curricula_configuration',
                                 dataset_name='curricula',
                                 input_file_path=Path(ENVIRONMENT_VARIABLES.get('CURRICULA_INPUT_DATA_FILE_PATH')),
                                 output_file_name=Path(ENVIRONMENT_VARIABLES.get('CURRICULA_DATA_OUTPUT_FILE_NAME')),
                                 output_directory_path=OUTPUT_DIRECTORY_PATH,
                                 order_by=['study_program_name',
                                           'study_program_duration',
                                           'course_name_mk'],
                                 columns_order=['study_program_name',
                                                'study_program_duration',
                                                'study_program_url',
                                                'course_code',
                                                'course_name_mk',
                                                'course_url',
                                                'course_type'],
                                 pipeline=run_curricula_pipeline,
                                 )

courses_config = Configuration(configuration_name='courses_configuration',
                               dataset_name='courses',
                               input_file_path=Path(ENVIRONMENT_VARIABLES.get('COURSES_INPUT_DATA_FILE_PATH')),
                               output_file_name=Path(ENVIRONMENT_VARIABLES.get('COURSES_DATA_OUTPUT_FILE_NAME')),
                               output_directory_path=OUTPUT_DIRECTORY_PATH,
                               order_by=['course_id'],
                               columns_order=['course_id',
                                              'course_code',
                                              'course_name_mk',
                                              'course_name_en',
                                              'course_academic_year',
                                              'course_semester',
                                              'course_level',
                                              'course_season',
                                              'course_prerequisite_type',
                                              'course_prerequisite',
                                              'course_professors',
                                              'course_prerequisite_ids',
                                              'course_professors_ids'],
                               pipeline=run_courses_pipeline,
                               )
