import streamlit as st
import os
from bilstein_slexa import config, local_data_input_path, log_output_path
from bilstein_slexa.pipeline.pipeline_manager import pipeline_run
import pandas as pd
import matplotlib.pyplot as plt
from streamlit_navigation_bar import st_navbar

# Constants
RAW_FOLDER = os.path.join(local_data_input_path, "tmp")
LOG_FOLDER = log_output_path
INFO_LOG_SUFFIX = ".info.log"
ERROR_LOG_SUFFIX = ".error.log"

# Set up page configuration - must be the first Streamlit command
st.set_page_config(page_title="Bilstein SLExA", layout="wide")


# CSS for styled buttons
st.markdown(
    """
    <style>
    .stButton > button {
        background-color: #6fb9ed; /* Blue background */
        color: white;
        padding: 10px 20px;
        font-size: 16px;
        border: none;
        border-radius: 8px;
        cursor: pointer;
        width: 150px;  /* Consistent button width */
        margin: 5px; /* Space between buttons */
    }
    .stButton > button:hover {
        background-color: #17659c; /* Darker green on hover */
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# Function to read log files
def read_log_file(file_path):
    try:
        with open(os.path.join(LOG_FOLDER, file_path), "r") as file:
            return file.read()
    except FileNotFoundError:
        return "Log file not found."


def display_header():
    st.markdown(
        """
        <style>
        .header { display: flex; align-items: center; justify-content: space-between; padding: 10px 0; border-bottom: 1px solid #ccc; margin-bottom: 20px; }
        .header .left-section { display: flex; align-items: center; }
        .header img { height: 80px; margin-right: 15px; }
        .header .project-name { font-size: 28px; font-weight: bold; color: #555; }
        .header .company-name { font-size: 24px; font-weight: bold; color: #333; margin: 0; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class="header">
            <div class="left-section">
                <img src="https://cdn.join.com/625431c0d46ec20008b9fae3/vanilla-steel-logo-xl.png" alt="Vanilla Steel">
                <span class="project-name">Bilstein SLExA©</span>
            </div>
            <h1 class="company-name">Vanilla Steel</h1>
        </div>
        """,
        unsafe_allow_html=True,
    )


def format_error_message(error_text):
    if isinstance(error_text, str):
        rows = error_text.split("\n")
        table = (
            f"{rows[0]}\n| "
            + " | ".join(rows[1].split())
            + " |\n"
            + "| --- " * len(rows[1].split())
            + "|\n"
        )
        for row in rows[1:]:
            table += "| " + " | ".join(row.split()) + " |\n"
        return table
    return f"**Warning Details**:\n\n```markdown\n{error_text}\n```"


def display_data_in_tabs(tabs, df_list, start, end):
    for tab, (status, df, filename, error_list, url) in zip(tabs, df_list[start:end]):
        with tab:
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.header(f"{filename}")
            with col3:
                if status:
                    table_size = df.shape
                else:
                    table_size = None
                data = {
                    "Metric": ["Status", "Table Size"],
                    "Value": [status, table_size],  # Example values
                }
                # Convert to DataFrame
                df_status = pd.DataFrame(data)
                # Display table in Streamlit
                st.table(df_status.reset_index(drop=True))

            col1, col2, col3 = st.columns([1, 3, 1])
            with col1:
                if url:
                    st.markdown(f"[Google Sheet]({url})")
            with col3:
                if status:
                    # Convert DataFrame to CSV
                    st.session_state[f"csv_{filename}"] = df.to_csv(index=False)

                    # Create a download button
                    st.download_button(
                        type="primary",
                        label="Download data as CSV",
                        key=f"dl_{filename}_btn",
                        data=st.session_state[f"csv_{filename}"],
                        file_name="data.csv",
                        mime="text/csv",
                    )
            if status:
                st.dataframe(df, height=400, hide_index=True)
            else:
                for error in error_list:
                    st.error(format_error_message(error))
                st.error("Please fix the errors and re-upload the document!")

            # Set up columns for Info and Error buttons
            col1, col2, col3 = st.columns([1, 4, 1])
            filename = filename.split(".")[0]
            with st.container(border=True):
                with col1:
                    # Display Info Log if button is clicked
                    if st.button(f"Info log", key=f"info_{filename}"):
                        st.session_state[f"info_log_{filename}"] = read_log_file(
                            f"{filename}{INFO_LOG_SUFFIX}"
                        )

                with col3:
                    # Display Error Log if button is clicked
                    if st.button(f"Error log", key=f"error_{filename}"):
                        st.session_state[f"error_log_{filename}"] = read_log_file(
                            f"{filename}{ERROR_LOG_SUFFIX}"
                        )

            # Show log content in expanders based on session state
            if st.session_state.get(f"info_log_{filename}"):
                with st.expander(f"Info: {filename}", expanded=True):
                    st.text(st.session_state[f"info_log_{filename}"])

            if st.session_state.get(f"error_log_{filename}"):
                with st.expander(f"Error: {filename}", expanded=True):
                    st.text(st.session_state[f"error_log_{filename}"])


# Main app function
def app():
    #  st.set_page_config(page_title="Bilstein SLExA", layout="wide")

    display_header()

    st.sidebar.header("Upload Excel Files")
    uploaded_files = st.sidebar.file_uploader(
        "Choose Excel files", type=["xlsx"], accept_multiple_files=True
    )

    # Check if pipeline has already been run
    if st.sidebar.button("Run Pipeline") and uploaded_files:
        # Save uploaded files and call pipeline
        for uploaded_file in uploaded_files:
            with open(os.path.join(RAW_FOLDER, uploaded_file.name), "wb") as f:
                f.write(uploaded_file.getbuffer())
        try:
            with st.spinner("Processing..."):
                dataframes = pipeline_run()
                st.session_state["dataframes"] = dataframes  # Store in session state
                st.session_state["show_info_message"] = True  # Show success message
                st.session_state["current_page"] = 1  # Reset to first page
        except Exception as e:
            st.error(f"An error occurred: {e}")

    # Show the data if already processed
    if "dataframes" in st.session_state:
        dataframes = st.session_state["dataframes"]
        passed_count = sum(1 for item in dataframes if item[0])
        failed_count = len(dataframes) - passed_count
        fig, ax = plt.subplots(facecolor="#f0f2f6")  # Set figure background color
        ax.set_facecolor("#f0f2f6")  # Set axes background color

        ax.pie(
            [passed_count, failed_count],
            labels=["Passed", "Failed"],
            autopct="%1.1f%%",
            startangle=90,
            colors=["#4CAF50", "#FF6347"],  # Green for passed, red for failed
        )
        ax.axis("equal")  # Equal aspect ratio ensures that pie is drawn as a circle.

        # Display the pie chart in the sidebar

        st.sidebar.markdown(
            "<div style='height: 100px;'></div>", unsafe_allow_html=True
        )  # Adjust 'height' as needed
        st.sidebar.pyplot(fig)

        if st.session_state.get("show_info_message", False):
            st.success("Data loaded successfully.")
            st.session_state["show_info_message"] = False  # Only show message once

        # Pagination setup
        tabs_per_page = 5
        num_pages = (len(dataframes) + tabs_per_page - 1) // tabs_per_page
        if "current_page" not in st.session_state:
            st.session_state["current_page"] = 1

        # Page navigation
        col1, col2, col3 = st.columns([1, 4, 1])
        with col1:
            previous_clicked = st.button("Previous")
        with col3:
            next_clicked = st.button("Next")

        if previous_clicked and st.session_state["current_page"] > 1:
            st.session_state["current_page"] -= 1
        # st.experimental_rerun()
        if next_clicked and st.session_state["current_page"] < num_pages:
            st.session_state["current_page"] += 1
        # st.experimental_rerun()
        with col2:
            st.markdown(
                f"<p style='text-align: center; font-size: 18px; font-weight: bold;'>"
                f"Page {st.session_state['current_page']} of {num_pages}</p>",
                unsafe_allow_html=True,
            )
        start = (st.session_state["current_page"] - 1) * tabs_per_page
        end = start + tabs_per_page

        tabs = st.tabs([item for _, _, item, _, _ in dataframes[start:end]])
        display_data_in_tabs(tabs, dataframes, start, end)


if __name__ == "__main__":
    app()
