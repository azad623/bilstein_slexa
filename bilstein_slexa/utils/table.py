import logging
import pandas as pd
from bilstein_slexa import config

logger = logging.getLogger("<Bilstein SLExA ETL>")


def find_potential_headers(df: pd.DataFrame) -> int:
    """
    Find potential header rows and select the best candidate based on string and non-null value distribution.

    Args:
        df (pd.DataFrame): Input DataFrame.
        config (dict): Configuration dictionary for BERT processing.

    Returns:
        int: Index of the identified header row.
    """

    potential_header_indices = []
    heuristic_header_idx = 0

    for idx, row in df.iterrows():
        non_null_count = row.notnull().sum()
        string_count = row.apply(lambda x: isinstance(x, str)).sum()

        # Identify rows where 70% are non-null and 50% are strings
        if (
            non_null_count > len(row) * config["row_density_threshold"]
            and string_count > len(row) * config["row_string_density_threshold"]
        ):
            potential_header_indices.append(idx)

    if potential_header_indices:
        heuristic_header_idx = max(
            potential_header_indices, key=lambda idx: df.iloc[idx].notnull().sum()
        )
        logger.info(f"The index of the header in Dataframe is: {heuristic_header_idx}")

    return heuristic_header_idx


def create_dataframe_from_table(
    table: pd.DataFrame, header_row_idx: int
) -> pd.DataFrame:
    """
    Create a DataFrame by extracting headers and table boundaries.

    Args:
        table (pd.DataFrame): Input DataFrame representing a table.
        header_row_idx (int): Index of the header row.

    Returns:
        pd.DataFrame: DataFrame with headers applied, cleaned of empty rows and columns.
    """

    try:
        df_with_headers = pd.DataFrame(
            table.values[header_row_idx:], columns=table.iloc[header_row_idx]
        )
        df_with_headers.dropna(how="all", inplace=True)
        df_with_headers = df_with_headers.reset_index(drop=True)

        # Drop the header row itself
        df_with_headers.drop(index=0, inplace=True)

        # Sanity check: Ensure the DataFrame has at least 2 columns and data
        if df_with_headers.empty or len(df_with_headers.columns) < 2:
            return None

        return df_with_headers
    except Exception as e:
        logger.error(f"Error creating DataFrame: {e}")
        return None


def identify_tables(df: pd.DataFrame) -> list:
    """
    Identify and extract tables from an Excel sheet, splitting by empty rows.

    Args:
        df (pd.DataFrame): DataFrame representing an Excel sheet.
        config (dict): Configuration dictionary for BERT header identification.

    Returns:
        list: List of DataFrames representing extracted tables.
    """
    extracted_tables = []

    if df.empty:
        logger.error("Input DataFrame is empty. No tables to extract.")
        return extracted_tables

    # Clean the current table and attempt to extract headers
    cleaned_table = df.dropna(how="all", axis=0).dropna(how="all", axis=1)
    header_idx = find_potential_headers(cleaned_table)
    table_df = create_dataframe_from_table(cleaned_table, header_idx)

    return table_df
