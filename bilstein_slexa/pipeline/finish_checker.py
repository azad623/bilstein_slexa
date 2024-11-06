import pandas as pd
import yaml
import logging
from bilstein_slexa import finish_repo_path

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
        df["finish2"] = None

        for idx, finish_id in df[finish_column].items():
            if finish_id in finish_dict:
                finish_data = finish_dict[finish_id]
                df.at[idx, finish_column] = finish_data[
                    "finish1"
                ]  # Update finish column with finish1
                df.at[idx, "finish2"] = finish_data.get(
                    "finish2"
                )  # Update finish2 if it exists
                logging.info(
                    f"Finish ID '{finish_id}' matched. Updated to '{finish_data['finish1']}'"
                )
            else:
                logging.warning(
                    f"Finish ID '{finish_id}'in row {idx+2} not found in the YAML data. No changes applied."
                )

        return df


# Example usage
if __name__ == "__main__":
    # Initialize FinishChecker with the path to the YAML file
    yaml_path = "path/to/your/finish_data.yaml"
    finish_checker = FinishChecker(yaml_path)

    # Sample DataFrame
    data = {"finish": [7, 10, 23, 999]}  # 999 represents a non-existing finish_id
    df = pd.DataFrame(data)

    # Run finish check and update
    updated_df = finish_checker.check_and_update_finish(df)
    print(updated_df)
