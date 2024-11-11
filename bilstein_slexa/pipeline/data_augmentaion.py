import pandas as pd
import logging
import numpy as np
from bilstein_slexa import config, global_vars

logger = logging.getLogger("<Bilstein SLExA ETL>")


def add_material_form(df: pd.DataFrame, threshold=600) -> pd.DataFrame:
    """
    Add a 'form' column to the DataFrame based on the width size.
    If width is greater than the threshold, 'form' is set to 'Coils', else 'Slit Coils'.

    Args:
        df (pd.DataFrame): The DataFrame to modify.
        width_column (str): The name of the column containing width values.
        threshold (float): The threshold value for categorizing width.

    Returns:
        pd.DataFrame: DataFrame with an added 'form' column.
    """
    # Initialize the 'form' column with NaN values
    df["form"] = np.nan
    column_name = "width"
    try:
        for idx, width_size in df[column_name].items():
            if isinstance(width_size, (float, int)):  # Check if width_size is numeric
                if width_size > threshold:
                    df.at[idx, "form"] = "Coils"
                else:
                    df.at[idx, "form"] = "Slit Coils"
            else:
                message = f"Non-numeric value encountered at index{df['bundle_id'].iloc[idx]} in '{column_name}': {width_size}"
                global_vars["error_list"].append(message)
                logger.warning()

        logger.info("The column 'form' was updated successfully.")
        return df

    except ValueError as e:
        logger.error(f"There was an error in determining material form: {e}")
        return df


def convert_warehouse_address(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update the 'location' column in the DataFrame based on a dictionary from config.
    If the location ID is found in the dictionary, it replaces the value in 'location';
    otherwise, it sets it to NaN and logs a warning.

    Args:
        df (pd.DataFrame): The DataFrame to modify.
        config (dict): Configuration dictionary containing 'template_data' with 'warehouse_address'.
        column_name (str): The column name to update in the DataFrame.

    Returns:
        pd.DataFrame: DataFrame with updated 'location' column.
    """
    # Check if 'choice' exists in the configuration
    if (
        "template_data" not in config
        or "warehause_address" not in config["template_data"]
    ):
        logger.error("The 'warehause_address' key is missing from the configuration.")
        return df
    else:
        add_dict = config["template_data"]["warehause_address"]
        column_name = "location"
        try:
            for idx, loc in df[column_name].items():
                if isinstance(loc, str):  # Ensure location ID is a string
                    if loc in add_dict:
                        df.at[idx, column_name] = add_dict[loc]
                    else:
                        df.at[idx, column_name] = np.nan
                        message = f"Location ID '{loc}' not found in YAML file. Bundle ID: {df['bundle_id'].iloc[idx]}"
                        global_vars["error_list"].append(message)
                        logger.warning(message)

                else:
                    logger.warning(
                        f"Non-string value encountered in '{column_name}' at Bundle ID {df['bundle_id'].iloc[idx]}: {loc}"
                    )

            logger.info("The 'location' column was updated successfully.")
            return df
        except ValueError as e:
            logger.error(f"Error in updating 'location' column: {e}")
            return df


def add_article_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update the 'article_id' column in the DataFrame by copying values from the 'bundle_id' column.
    If 'bundle_id' does not exist or an error occurs, logs the error.

    Args:
        df (pd.DataFrame): The DataFrame to modify.

    Returns:
        pd.DataFrame: DataFrame with the updated 'article_id' column.
    """
    try:
        # Update 'article_id' by copying 'bundle_id'
        df["article_id"] = df["bundle_id"]
        logger.info("The 'article_id' column was updated successfully.")
        return df

    except Exception as e:
        logger.error(f"Error in updating 'article_id' column: {e}")
        return df


def add_material_choice(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update the 'choice' column in the DataFrame based on the 'choice' value from the config.
    If 'choice' is not available in the config, logs an error.

    Args:
        df (pd.DataFrame): The DataFrame to modify.
        config (dict): Configuration dictionary containing 'template_data' with 'choice'.

    Returns:
        pd.DataFrame: DataFrame with the updated 'choice' column.
    """
    try:
        # Check if 'choice' exists in the configuration
        if "template_data" not in config or "choice" not in config["template_data"]:
            logger.error("The 'choice' key is missing from the configuration.")
            return df

        # Update the 'choice' column with the config value
        df["choice"] = config["template_data"]["choice"]
        logger.info("The 'choice' column was updated successfully.")
        return df

    except Exception as e:
        logger.error(f"Error in updating 'choice' column: {e}")
        return df


def add_access_default(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update the 'access' column in the DataFrame based on the 'access' value from the config.
    If 'access' is not available in the config, logs an error.

    Args:
        df (pd.DataFrame): The DataFrame to modify.
        config (dict): Configuration dictionary containing 'template_data' with 'access'.

    Returns:
        pd.DataFrame: DataFrame with the updated 'access' column.
    """
    try:
        # Verify 'template_data' and 'access' keys exist in the configuration
        access_value = config.get("template_data", {}).get("access")

        if access_value is None:
            logger.error("The 'access' key is missing from the configuration.")
            return df

        # Update the 'access' column with the config value
        df["access"] = access_value
        logger.info("The 'access' column was updated successfully.")
        return df

    except Exception as e:
        logger.error(f"Error in updating 'access' column: {e}")
        return df


def add_auction_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Update the 'auction_type' column in the DataFrame based on the 'action_type' value from the config.
    If 'action_type' is not available in the config, logs an error.

    Args:
        df (pd.DataFrame): The DataFrame to modify.
        config (dict): Configuration dictionary containing 'template_data' with 'action_type'.

    Returns:
        pd.DataFrame: DataFrame with the updated 'auction_type' column.
    """
    try:
        # Verify 'template_data' and 'action_type' keys exist in the configuration
        auction_type = config.get("template_data", {}).get("action_type")

        if auction_type is None:
            logger.error("The 'action_type' key is missing from the configuration.")
            return df

        # Update the 'auction_type' column with the config value
        df["auction_type"] = auction_type
        logger.info("The 'auction_type' column was updated successfully.")
        return df

    except Exception as e:
        logger.error(f"Error in updating 'auction_type' column: {e}")
        return df
