import streamlit as st

# Function for Home Page
def home():
    st.title("Home Page")
    st.write("Welcome to the main page of the GitHub Dashboard.")

# Function for GitHub Insights Page
def github_insights():
    st.title("GitHub Insights")
    st.write("This page will display GitHub repository insights.")

# Function for About Page
def about():
    st.title("About")
    st.write("This is an interactive Streamlit dashboard for GitHub data visualization.")

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Home", "GitHub Insights", "About"])

# Display Selected Page
if page == "Home":
    home()
elif page == "GitHub Insights":
    github_insights()
elif page == "About":
    about()
