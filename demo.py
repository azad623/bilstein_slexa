import streamlit as st


# Define a function to be called by the button
def show_info_message():
    st.session_state["show_info_message"] = True


def hide_info_message():
    st.session_state["show_info_message"] = False


def app():
    # Set initial state for showing/hiding the info message
    if "show_info_message" not in st.session_state:
        st.session_state["show_info_message"] = False

    # Button to show the message
    if st.button("Show Info Message"):
        show_info_message()  # This will set the message to show

    # Display the info message if the flag in session_state is True
    if st.session_state["show_info_message"]:
        st.info("This is an info message.")

        # Button to hide the message
        if st.button("Hide Info Message"):
            hide_info_message()  # This will set the message to hide


# Debugging: Show session state
st.write("Session State:", st.session_state)


if __name__ == "__main__()":
    app()
