import logging

import pandas as pd
from fastavro import validate


class SchemaValidationMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def validate_data(self, df: pd.DataFrame, schema: dict) -> pd.DataFrame:
        records_are_valid: bool = all(validate(record, schema) for record in df.to_dict(orient='records'))
        if records_are_valid:
            logging.info(f"Successfully validated records with schema {schema}")
            return df
        raise ValueError(f"Validation failed for schema: {schema}")
