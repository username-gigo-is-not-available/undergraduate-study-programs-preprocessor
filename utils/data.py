import pandas as pd


def merge_dataframes(df_curricula: pd.DataFrame, df_courses: pd.DataFrame) -> pd.DataFrame:
    merged_data = df_curricula.set_index('course_code').join(df_courses.set_index('course_code'), how='inner', lsuffix='_curricula', rsuffix='_courses')
    to_drop = [col for col in merged_data.columns if '_curricula' in col]
    to_rename = {col: col.replace('_courses', '') for col in merged_data.columns if '_courses' in col}
    return merged_data.drop(columns=to_drop).rename(columns=to_rename)

