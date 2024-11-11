import pandas as pd
import logging

logger = logging.getLogger("<Bilstein SLExA ETL>")


def validate_missing_values(df):
    required_columns = ["thickness(mm)", "width(mm)", "min_price", "weight"]
    missing_values_report = df[df[required_columns].isnull().any(axis=1)].reset_index(
        drop=True
    )
    if not missing_values_report.empty:
        logging.warning(
            "Missing values found in ('thickness(mm)', 'width(mm)', 'min_price', 'weight') columns."
        )
    return missing_values_report


def validate_units(df):
    unit_report = pd.DataFrame()
    if not all(df["thickness(mm)"].apply(lambda x: isinstance(x, (int, float)))):
        logging.error("Non-numeric value found in thickness.")
        unit_report = unit_report.append(
            {"column": "thickness", "error": "Non-numeric value"}, ignore_index=True
        )
    if not all(df["width(mm)"].apply(lambda x: isinstance(x, (int, float)))):
        logging.error("Non-numeric value found in width.")
        unit_report = unit_report.append(
            {"column": "width", "error": "Non-numeric value"}, ignore_index=True
        )
    return unit_report


def validate_frei_verwendbar(df):
    incorrect_units = df[df["weight"] <= 0]
    if not incorrect_units.empty:
        logging.error("Weight validation failed for 'Frei verwendbar'.")
        incorrect_units["error"] = "Weight not in valid range or incorrect unit"
    return incorrect_units
