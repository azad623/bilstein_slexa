import pandas as pd
import logging

logger = logging.getLogger("<Bilstein SLExA ETL>")

pd.set_option("display.max_columns", None)  # Ensure all columns are displayed
pd.set_option("display.width", 1000)
pd.set_option("display.max_rows", None)


def aggregate_data(df) -> tuple[bool, pd.DataFrame]:
    """
    Aggregates data grouped by 'bundle_id' and includes detailed information about unique columns.
    Validates that certain columns have identical values within each group.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Aggregated DataFrame with additional columns for unique values and validation results.
    """
    try:
        # Ensure 'thickness(mm)' and 'width(mm)' are used for aggregation
        required_columns = [
            "thickness(mm)",
            "width(mm)",
            "weight",
            "grade",
            "min_price",
            "location",
            "finish",
            "beschreibung",
        ]
        for col in required_columns:
            if col not in df.columns:
                raise KeyError(
                    f"The required column '{col}' is missing from the DataFrame."
                )

        # Group by 'bundle_id' and aggregate required metrics
        aggregated_df = (
            df.groupby("bundle_id")
            .agg(
                weight=("weight", "sum"),
                quantity=("weight", "count"),
                grade=(
                    "grade",
                    lambda x: ", ".join(x.unique()) if x.nunique() > 1 else x.iloc[0],
                ),
                finish=(
                    "finish",
                    lambda x: ", ".join(x.unique()) if x.nunique() > 1 else x.iloc[0],
                ),
                min_price=(
                    "min_price",
                    lambda x: (
                        ", ".join(map(str, x.unique()))
                        if x.nunique() > 1
                        else x.iloc[0]
                    ),
                ),
                location=(
                    "location",
                    lambda x: ", ".join(x.unique()) if x.nunique() > 1 else x.iloc[0],
                ),
                thickness=(
                    "thickness(mm)",
                    lambda x: (
                        ", ".join(map(str, x.unique()))
                        if x.nunique() > 1
                        else x.iloc[0]
                    ),
                ),
                width=(
                    "width(mm)",
                    lambda x: (
                        ", ".join(map(str, x.unique()))
                        if x.nunique() > 1
                        else x.iloc[0]
                    ),
                ),
                beschreibung=(
                    "beschreibung",
                    lambda x: ", ".join(x.unique()) if x.nunique() > 1 else x.iloc[0],
                ),
                description=(
                    "description",
                    lambda x: "\n".join(pd.Series(x.unique()).dropna()),
                ),
                batch_number=(
                    "batch_number",
                    lambda x: "\n".join(x.dropna().astype(str).unique()),
                ),
            )
            .reset_index()
        )

        # Validation: Check if values are not identical for specific columns
        validation_columns = [
            "grade",
            "min_price",
            "location",
            "finish",
            "thickness",
            "width",
            "beschreibung",
        ]

        aggregated_df_rep = pd.DataFrame()
        for col in validation_columns:
            aggregated_df_rep[f"{col}_identical"] = aggregated_df[col].apply(
                lambda x: len(set(x.split(", "))) == 1 if isinstance(x, str) else True
            )

        # Log warnings for non-identical values
        non_identical_rows_flag = True
        for col in validation_columns:
            non_identical_rows = aggregated_df[~aggregated_df_rep[f"{col}_identical"]]
            if not non_identical_rows.empty:
                logger.error(
                    f"Non-identical values detected in column '{col}' for some 'bundle_id' groups."
                )
                logger.error(
                    f"Details of non-identical rows:\n{non_identical_rows[['bundle_id', col]]}"
                )
                non_identical_rows_flag = False

        logger.info(
            "Data successfully aggregated with unique values and validation checks."
        )
        # df_string = aggregated_df.to_string(index=False)
        # logger.info(f"\n{aggregated_df_rep}")

        return non_identical_rows_flag, aggregated_df

    except KeyError as e:
        logger.error(f"KeyError - Missing column during aggregation: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during aggregation: {e}")
        raise
