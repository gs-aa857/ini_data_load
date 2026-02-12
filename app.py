import streamlit as st
import snowflake.connector
import pandas as pd
import datetime
import time
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
#@st.cache_resource(show_spinner=False)
def get_connection():
    try:
        
        return snowflake.connector.connect(
            user=st.secrets["snowflake"]["user"],
            #password=st.secrets["snowflake"]["password"],
            account=st.secrets["snowflake"]["account"],
            warehouse=st.secrets["snowflake"]["warehouse"],
            database=st.secrets["snowflake"]["database"],
            #schema=st.secrets["snowflake"]["schema"],
            private_key=st.secrets["snowflake"]["private_key"]
        )
    except Exception as e:
        st.error("Failed to connect to database.")
        #st.exception(e)
        return None
    
# ------------------------------
# Log query execution and errors
# ------------------------------
def log_query(user_id, view_id, start_date, end_date, number_of_rows, query_duration):
    try:
        conn = get_connection()
        query = f"""
        INSERT INTO {st.secrets['snowflake']['schema']}.USER_LOGS (user_id, view_id, action_timestamp, start_date, end_date, number_of_rows, query_duration)
        VALUES (%s, %s, CURRENT_TIMESTAMP(), %s, %s, %s, %s);
        """
        cursor = conn.cursor()
        cursor.execute(query, (user_id, view_id, start_date, end_date, number_of_rows, query_duration))
        conn.commit()
        cursor.close()
    except Exception as e:
        st.error("Error accessing database.")
        #st.exception(e)    
        
# ------------------------------
# Get user data from Snowflake
# ------------------------------
def get_user_data(email):
    try:
        conn = get_connection()
        query = f"""
        SELECT user_id, password FROM {st.secrets['snowflake']['schema']}.USERS WHERE username = '{email}'
        """
        cursor = conn.cursor()
        cursor.execute(query)
        user_data = cursor.fetchone()
        cursor.close()
        return user_data
    except Exception as e:
        st.error("Error identifying user.")
        #st.exception(e)
        return None

# ------------------------------
# Get user data from Snowflake
# ------------------------------
def get_user_views(email):
    try:
        conn = get_connection()
        query = f"""
        SELECT VIEWS.view_name, VIEWS.view_id, CONCAT(VIEWS.SCHEMA_NAME,'.',VIEWS.DB_VIEW_NAME) AS address
        FROM {st.secrets['snowflake']['schema']}.USER_VIEWS 
        JOIN {st.secrets['snowflake']['schema']}.USERS ON USER_VIEWS.user_id = USERS.user_id
        JOIN {st.secrets['snowflake']['schema']}.VIEWS ON USER_VIEWS.view_id = VIEWS.view_id
        WHERE LOWER(USERS.username) = %s
        """
        cursor = conn.cursor()
        cursor.execute(query, email)
        views = cursor.fetchall()
        cursor.close()

        if views:
            return {view[0]: [view[1],view[2]] for view in views}  # {view_name: address}
        else:
            return {}

    except Exception as e:
        st.error("Error accessing available views.")
        #st.exception(e)
        return {}

# ------------------------------
# Substract one month using datetime
# ------------------------------
def subtract_month(source_date):
    # 1. Calculate the target year and month
    month = source_date.month - 1
    year = source_date.year
    if month == 0:
        month = 12
        year -= 1

    # 2. Find the last day of the target month safely
    # We look at the 1st of the month AFTER our target, then subtract 1 day
    next_month = month % 12 + 1
    next_month_year = year if month < 12 else year + 1
    
    last_day_of_new_month = (
        datetime.datetime(next_month_year, next_month, 1) - datetime.timedelta(days=1)
    ).day

    # 3. Clip the day to the maximum allowed in the new month
    day = min(source_date.day, last_day_of_new_month)

    return datetime.datetime(year, month, day)

# ------------------------------
# login Interface
# ------------------------------

# Load secrets
# users = st.secrets.get("users", {})
# views = st.secrets.get("views", {})

# Initialize session state
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.email = None

# Login form
if not st.session_state.logged_in:
    email = (st.text_input("Email")).lower()
    password = st.text_input("Password", type="password")
    
    if st.button("Login"):
        try:
            
            if get_user_data(email)[1] == password:
                st.session_state.logged_in = True
                st.session_state.email = email
                st.rerun()  # Refresh the app
            else:
                st.error("Invalid email or password!")
        except Exception as e:
            st.error("Error during login.")
            #st.exception(e)

# ------------------------------
# Main App Interface (after succesful login)
# ------------------------------
if st.session_state.logged_in:
    
    st.success(f"Welcome, {st.session_state.email}!")

    user_views = get_user_views(st.session_state.email)
    selected_view = st.selectbox("Select a View", user_views.keys())
    
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
    
    # ------------------------------
    # Data Retrieval Button and Query Execution
    # ------------------------------
    if st.button("Get Data"):
        # Build the list of columns to retrieve: always include the hidden columns plus the user-selected ones.
        query = f"""
        SELECT *
        FROM {st.secrets['snowflake']['database']}.{user_views[selected_view][1]}
        WHERE TO_DATE(AD_DATE, 'DD.MM.YYYY') BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY TO_DATE(AD_DATE, 'DD.MM.YYYY')
        """
        try:
            with st.spinner("Querying Database..."):
                conn = get_connection()
                start_time = time.time()  # Start before query submission
                df = pd.read_sql(query, conn)
                end_time = time.time()
            
            # Store the DataFrame in session state to prevent re-querying
            st.session_state["df"] = df
            
            st.success("Data retrieved successfully!")
            
            # Log query details to the database
            log_query(
                user_id=get_user_data(st.session_state.email)[0],
                view_id=user_views[selected_view][0],
                start_date=start_date,
                end_date=end_date,
                query_duration=end_time - start_time,
                number_of_rows=df.shape[0]
            )

        except Exception as e:
            st.error(f"Error retrieving data.")
            #st.exception(e)
            
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



