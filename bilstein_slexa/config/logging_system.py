import logging
import yaml
import pandas as pd
from bilstein_slexa import log_config_path, log_output_path
from datetime import datetime
import os
from typing import Optional


# DataFrame to store detailed validation logs with scores
validation_log_df = pd.DataFrame(
    columns=["timestamp", "file", "row", "column", "value", "score", "message"]
)


def setup_logging(file_path: str, config: dict) -> logging.Logger:
    """
    Set up logging configuration with a dynamic file name.

    Args:
        file_name (str): The base name of the Excel file being processed.
    """
    # Load YAML configuration
    with open(log_config_path, "r") as file:
        logging_cfg = yaml.safe_load(file)

    # Remove the extension
    base_name = os.path.basename(file_path).split(".")[0]

    info_out_path = os.path.join(log_output_path, f"{base_name}.info.log")
    error_out_path = os.path.join(log_output_path, f"{base_name}.error.log")

    # Delete files if exist
    if config["etl_pipeline"]["rewrite_log"]:
        for path in [info_out_path, error_out_path]:
            if os.path.exists(path):
                os.remove(path)

    # Modify the filenames based on the Excel file name
    logging_cfg["handlers"]["info_file_handler"]["filename"] = info_out_path
    logging_cfg["handlers"]["error_file_handler"]["filename"] = error_out_path
    logging_cfg["handlers"]["warning_file_handler"]["filename"] = info_out_path

    # Apply the modified logging configuration
    logging.config.dictConfig(logging_cfg)
    logging.info(f"Logging initialized for {base_name}")
    logging.error(f"Logging initialized for {base_name}")

    # Sample logging
    logger = logging.getLogger("<Bilstein SLExA ETL>")
    return logger


def log_validation_result(
    file: str, row: int, column: str, value: any, score: float, message: str
):
    """
    Logs the validation result with a score for the value.

    Args:
        file (str): The file being processed.
        row (int): Row index of the value in the DataFrame.
        column (str): Column name of the value.
        value (any): The actual value being validated.
        score (float): Validation score (0 to 1) for the value.
        message (str): Log message, e.g., "Validation failed for column X".
    """
    timestamp = datetime.now()
    logging.info(
        f"VALIDATION - {file} - Row: {row}, Column: {column}, Value: {value}, Score: {score} - {message}"
    )
    validation_log_df.loc[len(validation_log_df)] = [
        timestamp,
        file,
        row,
        column,
        value,
        score,
        message,
    ]


def save_validation_log(output_path: str):
    """
    Save the validation log DataFrame to a file.

    Args:
        output_path (str): Path to save the log as a CSV file.
    """
    validation_log_df.to_csv(output_path, index=False)
