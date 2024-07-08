import pandas as pd

from pipeline.operations.clean_fields import clean_course_name_mk
from settings import ENVIRONMENT_VARIABLES

ECTS_VALUE: int = 6
PROFESSOR_TITLES: list[str] = ["ворн.", "проф.", "д-р", "доц."]
INVALID_COURSE_CODES: list[str] = ['F23L1S026', 'F23L2S066']
COURSE_NAMES: list[str] = (pd.read_csv(fr"{ENVIRONMENT_VARIABLES.get('COURSE_INPUT_DATA_FILE_PATH')}").get('course_name_mk')
                           .apply(clean_course_name_mk).tolist())
MAX_WORKERS: int = int(ENVIRONMENT_VARIABLES.get('MAX_WORKERS'))
