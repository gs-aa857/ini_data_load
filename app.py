import streamlit as st
import snowflake.connector
import pandas as pd
import datetime
import io

# ------------------------------
# Set page config and apply dark theme styling
# ------------------------------
#st.set_page_config(page_title="Initiative Data Downloader")
# Basic dark theme CSS for the Streamlit app
st.markdown(
    """
    <style>
    .reportview-container {
        background-color: #1e1e1e;
        color: #ffffff;
    }
    .sidebar .sidebar-content {
        background-color: #1e1e1e;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.image("Initiative_RGB_Blue.png", use_container_width=True)
st.title("Initiative EE Data Downloader")

# ------------------------------
# Snowflake Connection (secrets stored in .streamlit/secrets.toml)
# ------------------------------
@st.cache_resource(show_spinner=False)
def get_connection():
    try:
        
        return snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"]#,
            #schema=st.secrets["snowflake"]["schema"],
            #private_key=st.secrets["snowflake"]["private_key"]
        )
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None
    
# ------------------------------
# Get column names from a table (returns list of column names)
# ------------------------------
def get_table_columns(table_name):
    query = f"SELECT * FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{table_name} LIMIT 0"
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        cur.close()
        return cols
    except Exception as e:
        st.error(f"Error retrieving columns: {e}")
        return []

# ------------------------------
# Substract one month using datetime
# ------------------------------
def subtract_month(date):
    # Determine the new month and year
    new_month = date.month - 1
    new_year = date.year if new_month > 0 else date.year - 1
    new_month = new_month if new_month > 0 else 12  # Handle January going to December

    # Get the last day of the new month
    last_day_of_new_month = (datetime.datetime(new_year, new_month + 1, 1) - datetime.timedelta(days=1)).day

    # Adjust the day if the original day is greater than the last day of the new month
    new_day = min(date.day, last_day_of_new_month)

    return datetime.datetime(new_year, new_month, new_day)

# ------------------------------
# login Interface
# ------------------------------

# Load secrets
users = st.secrets.get("users", {})
views = st.secrets.get("views", {})

# Initialize session state
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.email = None

# Login form
if not st.session_state.logged_in:
    email = (st.text_input("Email")).lower()
    password = st.text_input("Password", type="password").lower()
    
    if st.button("Login"):
        user_data = users.get(email)  # Get user data if email exists
        
        if user_data and user_data["password"].lower() == password:
            st.session_state.logged_in = True
            st.session_state.email = email
            st.rerun()  # Refresh the app
        else:
            st.error("Invalid email or password!")

# ------------------------------
# Main App Interface (after succesful login)
# ------------------------------
if st.session_state.logged_in:
    
    st.success(f"Welcome, {st.session_state.email}!")

    user_views = users[st.session_state.email]["views"]
    selected_view = st.selectbox("Select a View", user_views)
    
    today = datetime.date.today()
    last_month = today.replace(day=1) - datetime.timedelta(days=1)
    default_start_date = subtract_month(last_month)
    default_end_date = last_month
    
    # Session state to store selected dates
    if "start_date" not in st.session_state:
        st.session_state.start_date = default_start_date
    if "end_date" not in st.session_state:
        st.session_state.end_date = default_end_date
    
    # Display date selectors with dynamic min/max values
    start_date = st.date_input(
        "Select Start Date", 
        value=st.session_state.start_date, 
        min_value=datetime.date(2019, 1, 1), 
        max_value=st.session_state.end_date  # Ensures start_date is not after end_date
    )
    
    end_date = st.date_input(
        "Select End Date", 
        value=st.session_state.end_date, 
        min_value=start_date,  # Ensures end_date is not before start_date
        max_value=today
    )
    
    # Update session state
    st.session_state.start_date = start_date
    st.session_state.end_date = end_date
    
    # Validation: Show message if selection is invalid
    if start_date > end_date:
        st.error("Start Date cannot be later than End Date. Please select a valid range.")
    else:
        st.success(f"Selected date range: **{start_date}** to **{end_date}**")
    
    st.write(f"You have selected: **{selected_view}**")
    
    # ------------------------------
    # Data Retrieval Button and Query Execution
    # ------------------------------
    if st.button("Get Data"):
        # Build the list of columns to retrieve: always include the hidden columns plus the user-selected ones.
        query = f"""
        SELECT *
        FROM {st.secrets['snowflake']['database']}.{views[selected_view]}
        WHERE TO_DATE(AD_DATE, 'DD.MM.YYYY') BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY TO_DATE(AD_DATE, 'DD.MM.YYYY')
        """
        try:
            with st.spinner("Querying Snowflake..."):
                conn = get_connection()
                df = pd.read_sql(query, conn)
                #conn.close()
            
            # Store the DataFrame in session state to prevent re-querying
            st.session_state["df"] = df
            
            st.success("Data retrieved successfully!")

        except Exception as e:
            st.error(f"Error retrieving data: {e}")
            
    if "df" in st.session_state:
        df = st.session_state["df"]
        
        st.dataframe(df.head(100))
        
        # ------------------------------
        # Download Format Selection: CSV or Excel
        # ------------------------------
        download_format = st.radio(f"Select Download Format{'' if df.shape[0] < 100000 else ' (generating Excel for large datasets can take time)'}", options=["Excel", "CSV"], index= 0 if df.shape[0] < 100000 else 1)
        
        if download_format == "CSV":
            with st.spinner("Generating CSV file..."):
                csv_data = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download as CSV",
                data=csv_data,
                file_name=f"{selected_view}.csv",
                mime="text/csv"
            )
            
        else:
            # Write DataFrame to an in-memory Excel file
            with st.spinner("Generating Excel file..."):
                towrite = io.BytesIO()
                with pd.ExcelWriter(towrite, engine="openpyxl") as writer:  # Use openpyxl instead of xlsxwriter
                    df.to_excel(writer, index=False, sheet_name="WeatherData")
 
            towrite.seek(0)
            st.download_button(
                label="Download as Excel",
                data=towrite,
                file_name=f"{selected_view}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
