import os
import yaml
from bilstein_slexa import logger, config, local_data_input_path
from bilstein_slexa.getters.data_getter import generate_path_list
from bilstein_slexa.getters.data_getter import load_excel_file
from bilstein_slexa.getters.schema_validation import validate_with_all_schemas
from bilstein_slexa.utils.table import identify_tables
from bilstein_slexa.utils.helper import save_pickle_file

# from bilstein_slexa.transformation import transform_data
from bilstein_slexa.config.logging_system import (
    setup_logging,
    log_validation_result,
    save_validation_log,
)


def pipeline_manager():
    """
    Orchestrates the ETL pipeline, managing each step sequentially.

    Args:
        config_path (str): Path to the YAML configuration file.
    """
    # schema_folder = config["schema_folder"]
    # output_folder = config["output_folder"]

    if config["etl_pipeline"]["run_extraction"]:
        excel_path_list = generate_path_list()
        if excel_path_list and len(excel_path_list) > 0:
            for file_path in excel_path_list:

                file_name = os.path.basename(file_path).split(".")[0]
                # Set up logging for each file
                logger = setup_logging(file_path)
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
                    # write funtion to delet Excel file in future
                    # del_excel_file(file_name)
        else:
            print(f"Could not find any valid Excel file in {local_data_input_path}")

    if config["etl_pipeline"]["run_extraction"]:
        pass
        # Step 4: Transform data
        # transformed_df = transform_data(df)

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
