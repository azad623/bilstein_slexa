import pandas as pd
import json
import logging
from bilstein_slexa import category_path

# Configure logger
logger = logging.getLogger("<Bilstein SLExA ETL>")

# Load validation data from JSON file
with open(category_path, "r") as f:
    validation_data = json.load(f)

# Convert JSON data to DataFrame
validation_df = pd.DataFrame(validation_data)


# Function for the equivalent of VLOOKUP
def vlookup(value, lookup_df, key_col, return_col):
    match = lookup_df[lookup_df[key_col] == value]
    return match[return_col].values[0] if not match.empty else None


# Apply the logic
def apply_logic(row):
    try:
        # Match row for J2 in column V
        match_row_idx = validation_df[validation_df["Forms"] == row["form"]].index
        if match_row_idx.empty:
            return ""

        # Match column for I2 in row W headers
        column_idx = validation_df.columns.get_loc(row["material"])
        if column_idx is None:
            return ""

        # Fetch value from INDEX equivalent
        cell_value = validation_df.iloc[match_row_idx[0], column_idx]

        if cell_value == "Finish":
            # Split K2 and get first part for VLOOKUP
            first_part = row["finish_1"].split(";")[0]
            return (
                vlookup(first_part, validation_df, "Finish Long", "Carbon Steel Flat")
                or ""
            )
        else:
            return cell_value

    except Exception as e:
        print(f"An error occurred: {e}")
        return ""


# Function to add 'material' column to DataFrame
def add_category(df: pd.DataFrame) -> pd.DataFrame:
    # Apply the function to each row
    df["category"] = df.apply(apply_logic, axis=1)
    return df
