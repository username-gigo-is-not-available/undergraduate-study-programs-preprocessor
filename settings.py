from dotenv import dotenv_values, load_dotenv

ENVIRONMENT_VARIABLES = dotenv_values(".env")

for variable_name, variable_value in ENVIRONMENT_VARIABLES.items():
    if not variable_value:
        raise RuntimeError(f"{variable_name} is not set!")