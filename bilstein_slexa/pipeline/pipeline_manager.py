import os
import yaml
import streamlit as st
from bilstein_slexa import (
    logger,
    config,
    local_data_input_path,
    log_output_path,
    source_schema_path,
    global_vars,
)
from bilstein_slexa.getters.data_getter import generate_path_list
from bilstein_slexa.getters.data_getter import load_excel_file
from bilstein_slexa.pipeline.schema_validation import (
    validate_with_all_schemas,
    get_required_columns,
    fix_data_types,
)
from bilstein_slexa.pipeline.transformation import (
    drop_rows_with_missing_values,
    standardize_missing_values,
    transform_dimensions,
    ensure_floating_point,
    translate_and_merge_description,
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
from bilstein_slexa.pipeline.data_augmentaion import (
    add_material_form,
    convert_warehouse_address,
    add_article_id,
    add_material_choice,
    add_access_default,
    add_auction_type,
    add_supplier_min,
)
from bilstein_slexa.pipeline.aggregation import aggregate_data
from bilstein_slexa.config.logging_system import setup_logger
from bilstein_slexa.utils.database import Database
from bilstein_slexa.pipeline.grade_checker import GradeChecker
from bilstein_slexa.pipeline.finish_checker import FinishChecker
from bilstein_slexa.pipeline.generate_gsheet import get_gsheet_url
from bilstein_slexa.utils.helper import delete_file, delete_all_files
from bilstein_slexa.pipeline.material_checker import add_material
from bilstein_slexa.pipeline.category_checker import add_category

# Define global variable to track the status of Excelsheet
status = None


def pipeline_run():
    """
    Orchestrates the ETL pipeline, managing each step sequentially.

    Args:
        config_path (str): Path to the YAML configuration file.
    """
    delete_all_files(os.path.join(local_data_input_path, "interim"))
    delete_all_files(log_output_path)

    if config["etl_pipeline"]["run_extraction"]:
        excel_path_list = generate_path_list(folder_name="tmp")
        if excel_path_list and len(excel_path_list) > 0:
            for file_path in excel_path_list:
                file_name = os.path.basename(file_path).split(".")[0]

                # Set up logging for each file
                global_vars["error_list"] = []
                status = False
                logger = setup_logger(file_path, config)
                logger.info(f"Starting processing for file: {file_path}")

                # Step 1: Load file
                logger.info("<< Step 1: Loading Excel from from pre-define location >>")
                df = load_excel_file(file_path)
                if df is None:
                    message = f"Loader failed to load Excel file to dataframe for: {file_path}"
                    global_vars["error_list"].append(message)
                    logger.error(message)
                    continue

                # Step 2: Detec the header suing heuristic approach
                # logger.info("<< Step 2: Detecting table's header >>")
                # df = identify_tables(df)

                # Step 3: Validate against multiple schemas with scoring
                logger.info(
                    "<< Step 3: Validate dataframe layout against pre-defined source schemas >>\n"
                )
                status = validate_with_all_schemas(df, file_path)
                print(f"error list >>{global_vars['error_list']}")
                save_pickle_file(
                    {
                        "file_name": file_name,
                        "data_frame": df,
                        "status": status,
                        "error_log": global_vars["error_list"],
                    },
                    file_name,
                    folder="interim",
                )

                # write funtion to delete Excel file
                delete_file(file_path)
        else:
            logger.error(
                f"Could not find any valid Excel file in {local_data_input_path}"
            )

        logger.info(f"Extration task is finished!\n\n")

    # Run transformation phase
    if config["etl_pipeline"]["run_transformation"]:

        # Setup the necessary path
        dir_path = os.path.join(local_data_input_path, "interim")
        schema = load_layout_schema(source_schema_path)
        delete_all_files(os.path.join(local_data_input_path, "processed"))

        required_cols = get_required_columns(schema)
        header_translations = {
            col["name"]: col["translation"]
            for col in schema["columns"]
            if col["mandatory"]
        }

        # Loop in pickle objects and read the dataframes

        for file_name in os.listdir(dir_path):
            status = False

            if os.path.isfile(os.path.join(dir_path, file_name)) and file_name.endswith(
                ".pk"
            ):
                item = load_pickle_file(os.path.join(dir_path, file_name))
                global_vars["error_list"] = []
                if item["status"]:

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
                    df.rename(columns=header_translations, inplace=True)

                    # Run transformations and validations
                    df = transform_dimensions(df)
                    df = ensure_floating_point(df)
                    not_missed, missing_values = validate_missing_values(df)

                    validation_reports = {
                        "missing_values": missing_values,
                        "unit_validation": validate_units(df),
                        "frei_verwendbar": validate_frei_verwendbar(df),
                    }

                    # Print validation reports
                    for report_name, report in validation_reports.items():
                        if not report.empty:
                            global_vars["error_list"].append(report)
                            logger.warning(f"\n{report_name.capitalize()} Report:")
                            logger.warning(f"\n{report}")

                    # Aggregate data grouped by 'Q-Meldungsnummer'
                    non_identical_rows_flag, aggregated_df = aggregate_data(df)

                    if non_identical_rows_flag and not_missed:
                        # Initialize the database connection
                        db = Database()
                        try:
                            # Translate description and merge columns[ description, bescheribung, batch_number]
                            df = translate_and_merge_description(aggregated_df)

                            # Check and update grade column
                            grade_checker = GradeChecker(db)
                            df = grade_checker.check_and_update_grade(
                                df, grade_column="grade"
                            )

                            # Check and update finish column
                            finish_checker = FinishChecker()
                            df = finish_checker.check_and_update_finish(
                                df, finish_column="finish"
                            )

                            # Add material form column
                            df = add_material_form(df)

                            # Convert the address code to real address
                            df = convert_warehouse_address(df)

                            # Add article ID column (same with bundle ID- only for internal usage)
                            df = add_article_id(df)

                            # Add material choice column (e,g 2nd, prime etc.)
                            df = add_material_choice(df)

                            # Add access default column
                            df = add_access_default(df)

                            # Add auction type column
                            df = add_auction_type(df)

                            # Add supplier min column (same with min_price)
                            df = add_supplier_min(df)

                            # Add material columns
                            df = add_material(df)

                            # Add category columns
                            df = add_category(df)

                            # Update status
                            status = True

                        except Exception as e:
                            print(e)
                        finally:
                            # Close the connection when done
                            db.close()
                            del grade_checker
                            del finish_checker

                    else:
                        df = None
                        logger.error(
                            f" >>> Fix the errors for Excel file {item['file_name']} and upload file again! <<<"
                        )
                else:
                    global_vars["error_list"] = item["error_log"]

                # write function to delete pickle file
                delete_file(os.path.join(dir_path, file_name))
                save_pickle_file(
                    {
                        "file_name": file_name,
                        "data_frame": df,
                        "status": status,
                        "error_log": global_vars["error_list"],
                    },
                    file_name,
                    folder="processed",
                )

    # Run loading Phase
    if config["etl_pipeline"]["run_loading"]:
        dataframes = []
        dir_path = os.path.join(local_data_input_path, "processed")
        for file_name in os.listdir(dir_path):
            url = None
            df = None
            if os.path.isfile(os.path.join(dir_path, file_name)) and file_name.endswith(
                ".pk"
            ):
                item = load_pickle_file(os.path.join(dir_path, file_name))
                if item["status"]:
                    df = item["data_frame"]
                    url = get_gsheet_url(
                        df,
                        file_name=item["file_name"],
                        folder_id=config["google_folder_id"],
                    )
                    logger.info(f"G-sheet URL :{url}")

                dataframes.append(
                    (item["status"], df, item["file_name"], item["error_log"], url)
                )
                delete_file(os.path.join(dir_path, file_name))

        print("ETL pipeline completed")
        return dataframes


if __name__ == "__main__":
    pipeline_run()
