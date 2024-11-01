import os
import pickle
import pandas as pd
import logging
import json
from bilstein_slexa import local_data_input_path

logger = logging.getLogger("<Bilstein SLExA ETL>")


def save_pickle_file(df: pd.DataFrame, file_name: str) -> None:
    """Save dataframe to pickle format

    Args:
        df (pd.DataFrame): Excel table converted to df
        file_name (str): the name of expected pickle file
    """
    try:
        with open(
            os.path.join(local_data_input_path, f"interim/{file_name}.pk"), "wb"
        ) as writer:
            pickle.dump(df, writer, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f"file {file_name} is dumped in data folder successfully")
    except Exception as e:
        logging.error(f"Could not save pickel file :{e}")


def load_pickle_file(file_name: str) -> pd.DataFrame:
    """Load the pickle file inrto dataframe

    Args:
        file_name (str): pickle filename

    Returns:
        pd.DataFrame: Excel dataftame
    """
    try:
        with open(
            os.path.join(local_data_input_path, "interim/{file_name}.pk"), "r"
        ) as reader:
            df = pickle.load(reader)
        logger.info(f"file {file_name} is loaded from data folder successfully")
        return df
    except Exception as e:
        logging.error(f"Could not load pickel file :{e}")


def load_layout_schema(schema_path: str) -> dict | None:
    """Load layout schema
    Args:
        schema_path (str): Path to the json schema

    Returns:
        dict : parsed json file
    """
    try:
        with open(schema_path, "r") as f:
            schema = json.load(f)
        return schema
    except ValueError as e:
        logger.error(f"could not load the schema file: {e}")
