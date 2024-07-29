import logging

import pandas as pd


def drop_and_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    columns_to_drop = [col for col in df.columns if '_x' in col]
    columns_to_rename = {col: col.replace('_y', '') for col in df.columns if '_y' in col}

    return df.drop(columns=columns_to_drop).rename(columns=columns_to_rename)


def merge_dataframes(df_curricula: pd.DataFrame, df_courses: pd.DataFrame, df_study_programs: pd.DataFrame) -> pd.DataFrame:
    logging.info("Joining dataframes df_study_programs and df_curricula on study_program_name and study_program_duration")
    merged_df = pd.merge(df_study_programs, df_curricula, how='inner', on=['study_program_name', 'study_program_duration'])
    merged_df = drop_and_rename_columns(merged_df)

    logging.info("Joining dataframes merged_df and df_courses on course_code")
    merged_df = pd.merge(merged_df, df_courses, how='inner', on='course_code')
    merged_df = drop_and_rename_columns(merged_df)

    return merged_df


