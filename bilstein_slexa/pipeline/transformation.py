import pandas as pd
import logging
import numpy as np

logger = logging.getLogger("<Bilstein SLExA ETL>")


def standardize_missing_values(df) -> pd.DataFrame:
    """
    Replace all common string representations of missing values with np.nan
    and ensure columns are in the correct numeric format where applicable.

    Args:
        df (pd.DataFrame): The input DataFrame to standardize.

    Returns:
        pd.DataFrame: DataFrame with standardized missing values.
    """
    # Replace common string representations of missing data with np.nan
    df.replace(["nan", "NaN", "N/A", "", "None"], np.nan, inplace=True)

    # Convert all columns to numeric if applicable, coercing errors to NaN
    # df = df.apply(lambda col: pd.to_numeric(col, errors='coerce') if col.dtype == 'object' else col)


def drop_rows_with_missing_values(df, required_columns, threshold):
    """
    Drop rows in place from the DataFrame where 90% (or a specified percentage) of the required column values are empty.

    Args:
        df (pd.DataFrame): The input DataFrame.
        required_columns (list): List of required columns to check for missing values.
        threshold (float): The threshold percentage (0.9 means 90%).

    Returns:
        None: The function modifies the DataFrame in place.
    """
    # Calculate the number of non-missing required columns needed to keep the row
    min_non_missing = int(len(required_columns) * (1 - threshold))

    # Filter rows based on the count of non-missing values in the required columns and drop them in place
    df.drop(
        df[df[required_columns].count(axis=1) <= min_non_missing].index, inplace=True
    )


def is_inconsistent_scale(value):
    """
    Check if the value seems to be in meters based on its format (e.g., values that start with 0.0XXX).

    Args:
        value (float): The value to be checked.

    Returns:
        bool: True if the value appears to be in meters, False otherwise.
    """
    # Assume values starting with 0.0XXX or very small numbers (< 0.1) might be in meters
    return 0 < value < 0.1


def convert_to_mm(value, column_name):
    """
    Convert value to mm if it's detected to be in meters.

    Args:
        value (float): The value to be checked and potentially converted.
        column_name (str): The name of the column for logging purposes.

    Returns:
        float: The value in mm.
    """
    if is_inconsistent_scale(value):
        logger.info(f"Converting {value} in column '{column_name}' from meters to mm.")
        return value * 1000
    return value


def transform_dimensions(df):
    """
    Ensure 'thickness' and 'width' columns are consistent and in mm scale.
    Convert from meters to mm if necessary and ensure no empty values.

    Args:
        df (pd.DataFrame): DataFrame with columns 'HF-Dicke' (thickness) and 'HF-Breite' (width).

    Returns:
        pd.DataFrame: Transformed DataFrame with consistency checks applied.
    """
    try:
        # Ensure columns are present
        required_columns = ["thickness(mm)", "width(mm)"]
        if not all(col in df.columns for col in required_columns):
            raise KeyError(f"Missing one or more required columns: {required_columns}")

        # Check and convert to mm scale if necessary
        df["thickness(mm)"].apply(lambda x: convert_to_mm(x, "thickness(mm)"))
        df["width(mm)"].apply(lambda x: convert_to_mm(x, "width(mm)"))

        logger.info(
            "Checked and converted 'thickness' and 'width' values to ensure they are in mm scale."
        )

        # Ensure columns are floating-point for consistency
        df["thickness(mm)"] = df["thickness(mm)"].astype(float)
        df["width(mm)"] = df["width(mm)"].astype(float)

        return df

    except KeyError as e:
        logger.error(f"KeyError - Missing column during transformation: {e}")
        raise
    except ValueError as e:
        logger.error(f"ValueError - Issue with data type conversion: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during transformation: {e}")
        raise


def ensure_floating_point(df):
    try:
        # Replace commas with dots and convert to float
        df["thickness(mm)"] = (
            df["thickness(mm)"].astype(str).str.replace(",", ".").astype(float)
        )
        df["width(mm)"] = (
            df["width(mm)"].astype(str).str.replace(",", ".").astype(float)
        )
        logging.info("Ensured dimensions are formatted as floating-point numbers.")
        return df
    except ValueError as e:
        logging.error(f"ValueError - Invalid conversion to float: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during float conversion: {e}")
        raise