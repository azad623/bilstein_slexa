import streamlit as st
import os, sys
from pathlib import Path
from bilstein_slexa import config, local_data_input_path, log_output_path
from bilstein_slexa.pipeline.pipeline_manager import pipeline_run


# Constants
RAW_FOLDER = os.path.join(
    local_data_input_path, "tmp"
)  # Folder where uploaded files are saved
LOG_FOLDER = log_output_path  # Folder where log files are stored
INFO_LOG_SUFFIX = ".info.log"
ERROR_LOG_SUFFIX = ".error.log"


# Run ETL Pipeline
def app():
    # Configure Streamlit page
    st.set_page_config(page_title="ETL Pipeline Runner", layout="wide")

    # Header
    st.title("ETL Pipeline Runner")

    # Sidebar for file upload
    st.sidebar.header("Upload Excel Files")
    uploaded_files = st.sidebar.file_uploader(
        "Choose Excel files", type=["xlsx"], accept_multiple_files=True
    )

    if st.sidebar.button("Run Pipeline"):
        if uploaded_files:
            st.info("Starting ETL pipeline...")

            # Save uploaded files to raw_folder
            for uploaded_file in uploaded_files:
                with open(os.path.join(RAW_FOLDER, uploaded_file.name), "wb") as f:
                    f.write(uploaded_file.getbuffer())

            # Call the pipeline function
            try:
                # Run the pipeline function and capture the results
                # info_log, warning_log, stats = pipeline_manager()
                pipeline_run()

                # Display logs and statistics in separate tabs
            except Exception as e:
                st.error(f"An error occurred: {e}")

        else:
            st.warning("Please upload at least one Excel file to run the pipeline.")


if __name__ == "__main__":
    app()
