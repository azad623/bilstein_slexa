import os
import json
import logging
from typing import Optional, List, Dict
import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from bilstein_slexa import config, source_schema_path
from bilstein_slexa.utils.helper import load_layout_schema

logger = logging.getLogger("<Bilstein SLExA ETL>")


def delete_extra_columns(df, required_columns) -> None:
    df.drop(columns=[col for col in df if col not in required_columns], inplace=True)


def get_required_columns(schema: Dict) -> List[str]:
    """
    Extract required column names from the schema.

    Args:
        schema (dict): The schema dictionary.

    Returns:
        List[str]: A list of required column names.
    """
    if not schema or "columns" not in schema:
        raise ValueError("Invalid schema format: missing 'columns' key.")

    required_columns = [
        col["name"]
        for col in schema.get("columns", [])
        if col.get("mandatory", False) is True
    ]
    return required_columns


def validate_with_all_schemas(df: pd.DataFrame, file_path: str):
    """
    Validate the DataFrame against multiple schemas in the specified folder. If no schema matches,
    log differences in column names and return None. If a schema matches, validate and fix data types
    and return the matched material values.

    Args:
        df (pd.DataFrame): The DataFrame loaded from the file.
        schema_folder (str): Path to the folder containing JSON schema files.
        file_path (str): The path to the file being validated.

    Returns:
        (bool, Optional[list]): True and material values if matched; otherwise, False and None.
    """
    unmatched_schemas = []

    # Normalize DataFrame column names (remove spaces and lowercase)
    df.columns = [col for col in df.columns]
    schema = load_layout_schema(source_schema_path)
    required_columns = get_required_columns(schema)

    # Check if all required columns are present using fuzzy matching
    matched_columns = match_and_fix_columns(df, required_columns)

    # Check if all required columns are present
    if required_columns == matched_columns:
        logger.info(
            f"Schema match found for {file_path} with schema {source_schema_path}"
        )

        # Match and fix data types
        df = fix_data_types(df, schema)

        # Delete uncessary columns from dataframe
        delete_extra_columns(df, required_columns)
        logger.info(f"Extra columns drops >> {df.columns}")

        return True

    # Collect differences for unmatched schemas
    unmatched_schemas.append(
        {
            "schema": source_schema_path,
            "missing_columns": [
                col for col in required_columns if col not in df.columns
            ],
        }
    )

    # Log report for unmatched schemas if no match was found
    logger.error(f"No matching schema found for {file_path}")
    for report in unmatched_schemas:
        logger.error(f"{report}")

    return False


def match_and_fix_columns(df: pd.DataFrame, required_columns: list) -> list:
    """
    Uses fuzzy matching to match and rename DataFrame columns to the schema's required columns
    if the match is close enough based on a specified threshold.

    Args:
        df (pd.DataFrame): DataFrame with columns to match.
        required_columns (list): List of required column names from the schema.
        match_threshold (int): Fuzzy matching threshold.

    Returns:
        list: List of columns that were matched and possibly renamed.
    """
    matched_columns = []
    for required_col in required_columns:
        # Fuzzy match each required column with DataFrame columns
        best_match, similarity = process.extractOne(
            required_col, df.columns, scorer=fuzz.ratio
        )

        if similarity >= config["column_match_threshold"]:
            matched_columns.append(required_col)
            if best_match != required_col:
                logger.info(
                    f"Renaming column '{best_match}' to '{required_col}' (similarity: {similarity}%)"
                )
                df.rename(columns={best_match: required_col}, inplace=True)

    return matched_columns


def clean_and_convert_to_string(df, columns):
    """
    Converts specified columns to strings, removing any `.0` suffix for floats.

    Args:
        df (pd.DataFrame): The DataFrame containing the columns to convert.
        columns (list): List of column names to clean and convert to strings.

    Returns:
        pd.DataFrame: DataFrame with specified columns converted to strings without `.0` suffixes.
    """
    for col in columns:
        # Convert float values that represent whole numbers to integers, then to strings
        df[col] = df[col].apply(
            lambda x: str(int(x)) if isinstance(x, float) and x.is_integer() else str(x)
        )
    return df


def fix_data_types(df: pd.DataFrame, schema: dict):
    """
    Validates and fixes data types based on the matched schema.

    Args:
        df (pd.DataFrame): The DataFrame containing the columns to validate and fix.
        schema (dict): The matched schema with columns and expected data types.
    """
    for column_info in schema.get("columns", []):
        column_status = column_info["mandatory"]
        expected_dtype = column_info["dtype"]
        column_name = column_info["name"]

        # Skip if column is missing or already of the correct type
        if column_name not in df.columns or column_status is False:
            continue
        actual_dtype = str(df[column_name].dtype)

        # Fix data type if mismatched
        if not is_dtype_match(expected_dtype, actual_dtype):
            try:
                df = convert_column_dtype(df, column_name, expected_dtype)
                logger.info(
                    f"Fixed column '{column_name}' to expected type '{expected_dtype}'"
                )
            except Exception as e:
                logger.error(
                    f"Failed to fix column '{column_name}' to type '{expected_dtype}': {e}"
                )
    return df


def is_dtype_match(expected_dtype: str, actual_dtype: str) -> bool:
    """
    Checks if the actual data type matches the expected data type.

    Args:
        expected_dtype (str): The expected data type as a string.
        actual_dtype (str): The actual data type of the column.

    Returns:
        bool: True if the data type matches, otherwise False.
    """
    dtype_map = config["dtype_map"]
    return dtype_map.get(expected_dtype) in actual_dtype


def convert_column_dtype(
    df: pd.DataFrame, column_name: str, expected_dtype: str
) -> pd.DataFrame:
    """
    Converts the column to the expected data type if possible.

    Args:
        df (pd.DataFrame): The DataFrame containing the column.
        column_name (str): The name of the column to convert.
        expected_dtype (str): The expected data type as specified in the schema.

    Returns:
        pd.DataFrame: The DataFrame with the updated column.
    """
    if expected_dtype == "string":
        df[column_name] = df[column_name].astype(pd.StringDtype())
    elif expected_dtype == "float":
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce")
    elif expected_dtype == "int":
        df[column_name] = pd.to_numeric(df[column_name], errors="coerce").astype(
            "Int64"
        )
    elif expected_dtype == "boolean":
        df[column_name] = df[column_name].astype(bool)
    elif expected_dtype == "date":
        df[column_name] = pd.to_datetime(df[column_name], errors="coerce")
    return df
