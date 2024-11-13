import pandas as pd
import yaml
import logging
from bilstein_slexa import finish_repo_path, global_vars
import numpy as np

# Configure logging
logger = logging.getLogger("<Bilstein SLExA ETL>")


class FinishChecker:
    def __init__(self):
        """
        Initialize FinishChecker by loading finishes from a YAML file.

        Args:
            yaml_path (str): Path to the YAML file containing finish data.
        """
        self.finishes = self.load_finishes_from_yaml()

    def load_finishes_from_yaml(self):
        """Load finish data from a YAML file."""
        with open(finish_repo_path, "r") as file:
            return yaml.safe_load(file)

    def check_and_update_finish(self, df, finish_column="finish"):
        """
        Check and update finishes in a DataFrame based on loaded finish data.

        Args:
            df (pd.DataFrame): The DataFrame to update.
            finish_column (str): The name of the column to check and update.

        Returns:
            pd.DataFrame: Updated DataFrame with `finish1` and `finish2` values.
        """
        # Create a lookup dictionary from finish data for faster access
        finish_dict = {str(item["finish_id"]): item for item in self.finishes}

        # Initialize finish2 column
        # df["finish_2"] = np.nan

        for idx, finish_id in df[finish_column].items():
            if finish_id in finish_dict:
                finish_data = finish_dict[finish_id]
                df.at[idx, finish_column] = finish_data[
                    "finish_1"
                ]  # Update finish column with finish1
                # df.at[idx, "finish_2"] = finish_data.get(
                #     "finish_2"
                # )  # Update finish2 if it exists
                logger.info(
                    f"Finish ID '{finish_id}' matched. Updated to '{finish_data['finish_1']}'"
                )
            else:
                message = f"Finish ID '{finish_id}' not found in Bundle Id {df['bundle_id'].loc[idx]} in the YAML data. Updated to 'NaN'"
                df.at[idx, finish_column] = np.nan
                global_vars["error_list"].append(message)
                logger.warning(message)
        df.rename(columns={finish_column: "finish_1"}, inplace=True)
        return df
