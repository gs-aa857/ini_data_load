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
        private_key_p8 = st.secrets["snowflake"]["private_key"]
        # private_key_base64 = st.secrets["snowflake"]["private_key"]
        # private_key_p8 = base64.b64decode(private_key_base64).decode("utf-8")
        
        return snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            # password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            schema=st.secrets["snowflake"]["schema"],
            private_key=private_key_p8
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
    start_date = subtract_month(last_month)
    end_date = last_month
    date_range = st.date_input(
        "Select Date Range",
        value=(start_date, end_date),
        min_value=datetime.date(2019, 1, 1),
        max_value=today
    )
    
    st.write(f"You have selected: **{selected_view}**")
    
    # ------------------------------
    # Data Retrieval Button and Query Execution
    # ------------------------------
    if st.button("Get Data"):
        # Build the list of columns to retrieve: always include the hidden columns plus the user-selected ones.
        query = f"""
        SELECT *
        FROM {st.secrets['snowflake']['database']}.{st.secrets['snowflake']['schema']}.{views[selected_view]}
        WHERE TO_DATE(AD_DATE, 'DD.MM.YYYY') BETWEEN '{date_range[0]}' AND '{date_range[1]}'
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
            st.dataframe(df)

        except Exception as e:
            st.error(f"Error retrieving data: {e}")
            
    if "df" in st.session_state:
        df = st.session_state["df"]
        
        # ------------------------------
        # Download Format Selection: CSV or Excel
        # ------------------------------
        download_format = st.radio("Select Download Format", options=["Excel", "CSV"])
        if download_format == "CSV":
            csv_data = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="Download as CSV",
                data=csv_data,
                file_name="{selected_view}.csv",
                mime="text/csv"
            )
            
        else:
            # Write DataFrame to an in-memory Excel file
            towrite = io.BytesIO()
            with pd.ExcelWriter(towrite, engine="openpyxl") as writer:  # Use openpyxl instead of xlsxwriter
                df.to_excel(writer, index=False, sheet_name="WeatherData")
            # with pd.ExcelWriter(towrite, engine='xlsxwriter') as writer:
            #     df.to_excel(writer, index=False, sheet_name="WeatherData")
            #     writer.save()
            towrite.seek(0)
            st.download_button(
                label="Download as Excel",
                data=towrite,
                file_name="{selected_view}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
