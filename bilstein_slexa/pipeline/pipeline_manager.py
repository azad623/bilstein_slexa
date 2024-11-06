import os
import yaml
from bilstein_slexa import logger, config, local_data_input_path, source_schema_path
from bilstein_slexa.getters.data_getter import generate_path_list
from bilstein_slexa.getters.data_getter import load_excel_file
from bilstein_slexa.pipeline.schema_validation import (
    validate_with_all_schemas,
    get_required_columns,
    fix_data_types,
)
from bilstein_slexa.pipeline.aggregation import aggregate_data

# from bilstein_slexa.utils.table import identify_tables
from bilstein_slexa.pipeline.transformation import (
    drop_rows_with_missing_values,
    standardize_missing_values,
    transform_dimensions,
    ensure_floating_point,
)
from bilstein_slexa.utils.helper import (
    save_pickle_file,
    load_layout_schema,
    load_pickle_file,
)
from bilstein_slexa.pipeline.data_validation import (
    validate_frei_verwendbar,
    validate_missing_values,
    validate_units,
)

# from bilstein_slexa.transformation import transform_data
from bilstein_slexa.config.logging_system import setup_logger

from bilstein_slexa.utils.database import Database
from bilstein_slexa.pipeline.grade_checker import GradeChecker
from bilstein_slexa.pipeline.finish_checker import FinishChecker


def pipeline_manager():
    """
    Orchestrates the ETL pipeline, managing each step sequentially.

    Args:
        config_path (str): Path to the YAML configuration file.
    """

    if config["etl_pipeline"]["run_extraction"]:
        excel_path_list = generate_path_list(folder_name="raw")
        if excel_path_list and len(excel_path_list) > 0:
            for file_path in excel_path_list:
                file_name = os.path.basename(file_path).split(".")[0]

                # Set up logging for each file
                logger = setup_logger(file_path, config)
                logger.info(f"Starting processing for file: {file_path}")

                # Step 1: Load file
                logger.info("<< Step 1: Loading Excel from from pre-define location >>")
                df = load_excel_file(file_path)
                if df is None:
                    logger.error(
                        f"Loader failed to load Excel file to dataframe for: {file_path}"
                    )
                    continue

                # Step 2: Detec the header suing heuristic approach
                # logger.info("<< Step 2: Detecting table's header >>")
                # df = identify_tables(df)

                # Step 3: Validate against multiple schemas with scoring
                logger.info(
                    "<< Step 3: Validate dataframe layout against pre-defined source schemas >>\n"
                )
                if validate_with_all_schemas(df, file_path):
                    save_pickle_file(
                        {"file_name": file_name, "data_frame": df}, file_name
                    )
                    # write funtion to delete
                    #  Excel file in future
                    # del_excel_file(file_name)
        else:
            print(f"Could not find any valid Excel file in {local_data_input_path}")

        logger.info(f"Extration task is finished!\n\n")

    if config["etl_pipeline"]["run_transformation"]:
        # Step 4: Transform data

        # Setup the necessary path
        dir_path = os.path.join(local_data_input_path, "interim")
        schema = load_layout_schema(source_schema_path)

        required_cols = get_required_columns(schema)
        translations = {
            col["name"]: col["translation"]
            for col in schema["columns"]
            if col["mandatory"]
        }

        # Loop in pickle objects and read the dataframes
        for file_name in os.listdir(dir_path):
            if os.path.isfile(os.path.join(dir_path, file_name)) and file_name.endswith(
                ".pk"
            ):
                item = load_pickle_file(os.path.join(dir_path, file_name))
                df = item["data_frame"]

                # Set up logging for each file
                logger = setup_logger(item["file_name"], config)

                # Fix data type after loading pickle file
                df = fix_data_types(df, schema)

                # Convert all empty values to NAN
                standardize_missing_values(df)

                # Drop rows when 90% of the required row values are empty
                drop_rows_with_missing_values(df, required_cols, threshold=0.9)

                # Rename columns based on translations
                df.rename(columns=translations, inplace=True)

                # Run transformations and validations
                df = transform_dimensions(df)
                df = ensure_floating_point(df)

                validation_reports = {
                    "missing_values": validate_missing_values(df),
                    "unit_validation": validate_units(df),
                    "frei_verwendbar": validate_frei_verwendbar(df),
                }

                # Print validation reports
                for report_name, report in validation_reports.items():
                    if not report.empty:
                        logger.warning(f"\n{report_name.capitalize()} Report:")
                        logger.warning(f"\n{report}")

                # Aggregate data grouped by 'Q-Meldungsnummer'
                non_identical_rows_flag, aggregated_df = aggregate_data(df)
                grades = aggregated_df["grade"].to_list()
                print(grades)

                # material_spec_flag, material_updated_df = validate_grade_finish(df)

                if non_identical_rows_flag:
                    # Initialize the database connection
                    db = Database()

                    try:
                        grade_checker = GradeChecker(db)
                        df = grade_checker.check_and_update_grade(
                            df, grade_column="grade"
                        )

                        finish_checker = FinishChecker()
                        df = finish_checker.check_and_update_finish(
                            df, finish_column="finish"
                        )
                    except Exception as e:
                        print(e)
                    finally:
                        # Close the connection when done
                        db.close()
                        del grade_checker
                        del finish_checker

        # Step 5: Save the processed file
        # output_file = os.path.join(output_folder, f"processed_{file_name}")
        # transformed_df.to_excel(output_file, index=False)
        # log_system_event("INFO", f"File processed and saved as: {output_file}")

    if config["etl_pipeline"]["run_extraction"]:
        pass
        # Save the detailed validation log with scores
        # log_output_path = os.path.join(
        #    output_folder, f"{os.path.splitext(file_name)[0]}_validation_log.csv"
        # )
        # save_validation_log(log_output_path)

    print("ETL pipeline completed")


if __name__ == "__main__":
    pipeline_manager()
