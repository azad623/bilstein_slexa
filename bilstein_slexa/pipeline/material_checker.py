import pandas as pd
import json
import logging
from bilstein_slexa import material_schema_path

# Configure logger
logger = logging.getLogger("<Bilstein SLExA ETL>")

# Load validation data from JSON file
with open(material_schema_path, "r") as f:
    validation_data = json.load(f)

# Convert JSON data to DataFrame
validation_df = pd.DataFrame(validation_data)


# Function to perform the lookup based on 'Grade_Suffix', 'Grade', and 'Suffix'
def lookup_material(value, lookup_df):
    # Check for matches in 'Grade_Suffix', 'Grade', and 'Suffix'
    match = lookup_df[
        (lookup_df["Grade_Suffix"] == value)
        | (lookup_df["Grade"] == value)
        | (lookup_df["Suffix"] == value)
    ]
    if not match.empty:
        return match["Material"].values[0]
    else:
        logger.warning(f"No material found for grade '{value}'")
        return None  # Return None when no match is found


# Apply the logic
def apply_logic(row):
    # Check the conditions for 'choice', 'form', and empty 'grade'
    if (row["choice"] == "3rd" or row["form"] == "Offcuts") and row["grade"] == "":
        return "Carbon Steel"
    else:
        # Perform lookup for 'grade' in the validation DataFrame
        return lookup_material(row["grade"], validation_df)


# Function to add 'material' column to DataFrame
def add_material(df: pd.DataFrame) -> pd.DataFrame:
    # Apply the function to each row
    df["material"] = df.apply(apply_logic, axis=1)
    return df
