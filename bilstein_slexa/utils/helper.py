import os
import pickle
import pandas as pd
import logging
import json
from bilstein_slexa import local_data_input_path

logger = logging.getLogger("<Bilstein SLExA ETL>")


def save_pickle_file(df: pd.DataFrame, file_name: str, folder="interim") -> None:
    """Save dataframe to pickle format

    Args:
        df (pd.DataFrame): Excel table converted to df
        file_name (str): the name of expected pickle file
    """
    try:
        with open(
            os.path.join(local_data_input_path, f"{folder}/{file_name}.pk"), "wb"
        ) as writer:
            pickle.dump(df, writer, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f"file {file_name} is dumped in data folder successfully")
    except Exception as e:
        logging.error(f"Could not save pickel file :{e}")


def load_pickle_file(file_path: str) -> pd.DataFrame:
    """Load the pickle file inrto dataframe

    Args:
        file_name (str): pickle filename

    Returns:
        pd.DataFrame: Excel dataftame
    """
    try:
        with open(file_path, "rb") as f:
            data = pickle.load(f)
            if (
                not isinstance(data, dict)
                or "data_frame" not in data
                or "file_name" not in data
            ):
                raise ValueError(
                    "Pickle file must contain a dictionary with 'file_name' and 'data_frame' keys."
                )
        logging.info(f"Successfully loaded data from {file_path}")
        return data
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except pickle.PickleError:
        logging.error("Failed to load pickle file due to an unpickling error.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


def load_layout_schema(schema_path: str) -> dict | None:
    """Load layout schema
    Args:
        schema_path (str): Path to the json schema

    Returns:
        dict : parsed json file
    """
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)

        # Validate schema structure if required keys are expected
        if not isinstance(schema, dict):
            raise ValueError("The schema must be a dictionary at the root level.")
        if "columns" not in schema or not isinstance(schema["columns"], list):
            raise ValueError(
                "The schema must contain a 'columns' key with a list of column definitions."
            )

        logger.info("Schema successfully loaded and validated.")
        return schema

    except FileNotFoundError:
        logger.error(f"Schema file not found: {schema_path}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON file: {e}")
    except ValueError as e:
        logger.error(f"Schema validation error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading the schema: {e}")


def delete_file(file_path):
    """
    Deletes the specified file if it exists and logs the action.

    Args:
        file_path (str): The path of the file to be deleted.

    Raises:
        ValueError: If the provided path is not a file.
    """
    # Sanity check: Ensure the file path is valid
    if not isinstance(file_path, str):
        logging.error("Invalid input: file_path should be a string.")

    # Check if file exists
    if not os.path.exists(file_path):
        logging.warning(f"File not found: {file_path}. No action taken.")

    # Check if the path is a file and not a directory
    if not os.path.isfile(file_path):
        logging.error(f"The provided path is not a file: {file_path}")
        raise ValueError("The provided path is not a file.")

    try:
        # Attempt to delete the file
        os.remove(file_path)
        logging.info(f"File deleted successfully: {file_path}")
    except Exception as e:
        logging.error(f"Error deleting file {file_path}: {e}")


def delete_all_files(folder_path):
    """
    Delete all files in the specified folder.

    Args:
        folder_path (str): Path to the folder.

    Returns:
        None
    """
    try:
        # List all files in the folder
        files = [
            os.path.join(folder_path, file)
            for file in os.listdir(folder_path)
            if os.path.isfile(os.path.join(folder_path, file))
        ]

        # Delete each file
        for file in files:
            os.remove(file)
            print(f"Deleted: {file}")

        print("All files have been deleted.")

    except FileNotFoundError:
        print(f"The folder '{folder_path}' does not exist.")
    except PermissionError:
        print(f"Permission denied to delete files in '{folder_path}'.")
    except Exception as e:
        print(f"An error occurred: {e}")
