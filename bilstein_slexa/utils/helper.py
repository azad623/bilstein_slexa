import os
import pickle
import pandas as pd
import logging
from bilstein_slexa import local_data_input_path

logger = logging.getLogger("<Bilstein SLExA ETL>")


def save_pickle_file(df: pd.DataFrame, file_name: str) -> None:
    try:
        with open(
            os.path.join(local_data_input_path, "interim/{file_name}.pk"), "wb"
        ) as writer:
            pickle.dump(df, writer, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f"file {file_name} is dumped in data folder successfully")
    except Exception as e:
        logging.error(f"Could not save pickel file :{e}")


def load_pickle_file(file_name: str) -> pd.DataFrame:
    try:
        with open(
            os.path.join(local_data_input_path, "interim/{file_name}.pk"), "r"
        ) as reader:
            pickle.load(reader)
        logger.info(f"file {file_name} is loaded from data folder successfully")
    except Exception as e:
        logging.error(f"Could not load pickel file :{e}")
