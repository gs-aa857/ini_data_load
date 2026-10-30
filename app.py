import calendar
import datetime
import hmac
import io
import logging
import re
import time

import bcrypt
import pandas as pd
import snowflake.connector
from snowflake.connector import errors as sf_errors
import streamlit as st

# ------------------------------
# Logging: errors go to the Streamlit Cloud log, users see a short message
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("ini_data_load")

# ------------------------------
# Settings
# ------------------------------
SF = st.secrets["snowflake"]
SCHEMA = SF["schema"]
DATABASE = SF["database"]

EXCEL_MAX_ROWS = 1_048_575   # Excel sheet limit (1,048,576) minus the header row
EXCEL_SOFT_LIMIT = 100_000   # above this, default to CSV because Excel generation is slow
SESSION_EXPIRED_ERRNO = 390114  # Snowflake "authentication token has expired"

# ------------------------------
# Page header
# ------------------------------
st.image("Initiative_RGB_Blue.png", width="stretch")
st.title("Initiative EE Data Downloader")


# ------------------------------
# Snowflake connection (secrets stored in .streamlit/secrets.toml)
# One shared connection, reused across reruns instead of a new one per call.
# ------------------------------
@st.cache_resource(show_spinner=False)
def _connect():
    # Raises on failure, so a failed connection is never cached.
    return snowflake.connector.connect(
        user=SF["user"],
        account=SF["account"],
        warehouse=SF["warehouse"],
        database=SF["database"],
        private_key=SF["private_key"],
        client_session_keep_alive=True,
    )


def get_connection():
    conn = _connect()
    if conn.is_closed():
        _connect.clear()
        conn = _connect()
    return conn


def run_query(sql, params=None, fetch="all"):
    """Run a parameterised query. Reconnects once if the session has dropped.

    fetch: "all" -> list of rows, "one" -> single row, "df" -> DataFrame, None -> nothing
    """
    for attempt in (1, 2):
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
                if fetch == "df":
                    columns = [c[0] for c in cur.description]
                    # coerce_float matches the old pd.read_sql behaviour (NUMBER -> float)
                    return pd.DataFrame.from_records(cur.fetchall(), columns=columns, coerce_float=True)
                return None
        except sf_errors.Error as e:
            retryable = isinstance(e, (sf_errors.OperationalError, sf_errors.InterfaceError)) or (
                getattr(e, "errno", None) == SESSION_EXPIRED_ERRNO
            )
            if attempt == 2 or not retryable:
                raise
            log.warning("Snowflake connection problem, reconnecting", exc_info=True)
            _connect.clear()


# ------------------------------
# Users, passwords and views
# ------------------------------
def get_user(email):
    """Returns (user_id, stored_password) or None if the user does not exist."""
    return run_query(
        f"SELECT user_id, password FROM {SCHEMA}.USERS WHERE LOWER(username) = %s",
        (email,),
        fetch="one",
    )


def verify_password(stored, given):
    """Accepts bcrypt hashes; falls back to plain-text comparison for passwords not yet migrated."""
    if not stored or not given:
        return False
    if stored.startswith(("$2a$", "$2b$", "$2y$")):
        return bcrypt.checkpw(given.encode("utf-8"), stored.encode("utf-8"))
    return hmac.compare_digest(stored.encode("utf-8"), given.encode("utf-8"))


@st.cache_data(ttl=600, show_spinner=False)
def get_user_views(email):
    """Returns {view_name: (view_id, schema.view)} for the views this user may access."""
    rows = run_query(
        f"""
        SELECT VIEWS.view_name, VIEWS.view_id, CONCAT(VIEWS.SCHEMA_NAME, '.', VIEWS.DB_VIEW_NAME) AS address
        FROM {SCHEMA}.USER_VIEWS
        JOIN {SCHEMA}.USERS ON USER_VIEWS.user_id = USERS.user_id
        JOIN {SCHEMA}.VIEWS ON USER_VIEWS.view_id = VIEWS.view_id
        WHERE LOWER(USERS.username) = %s
        ORDER BY VIEWS.view_name
        """,
        (email,),
    )
    return {name: (view_id, address) for name, view_id, address in rows}


def log_query(user_id, view_id, start_date, end_date, number_of_rows, query_duration):
    """Usage logging. A failure here is logged but never shown to the user."""
    try:
        run_query(
            f"""
            INSERT INTO {SCHEMA}.USER_LOGS
                (user_id, view_id, action_timestamp, start_date, end_date, number_of_rows, query_duration)
            VALUES (%s, %s, CURRENT_TIMESTAMP(), %s, %s, %s, %s)
            """,
            (user_id, view_id, start_date, end_date, int(number_of_rows), float(query_duration)),
            fetch=None,
        )
    except Exception:
        log.exception("Failed to write usage log")


# ------------------------------
# Helpers
# ------------------------------
def subtract_month(source_date):
    """Same date one month earlier, clipped to the last day of that month."""
    if source_date.month == 1:
        year, month = source_date.year - 1, 12
    else:
        year, month = source_date.year, source_date.month - 1
    last_day = calendar.monthrange(year, month)[1]
    return datetime.date(year, month, min(source_date.day, last_day))


def safe_filename(text):
    return re.sub(r"[^\w\-. ]", "_", str(text)).strip() or "data"


def build_file(df, fmt):
    if fmt == "CSV":
        # utf-8-sig so Excel opens non-ASCII characters (õ, ä, ö, ü, š, ž) correctly
        return df.to_csv(index=False).encode("utf-8-sig")

    out = df.copy()
    # Excel cannot store timezone-aware datetimes (Snowflake TIMESTAMP_TZ / LTZ)
    for col in out.select_dtypes(include=["datetimetz"]).columns:
        out[col] = out[col].dt.tz_localize(None)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="Data")
    return buf.getvalue()


def logout():
    for key in list(st.session_state.keys()):
        del st.session_state[key]


# ------------------------------
# Login interface
# ------------------------------
if not st.session_state.get("logged_in"):
    with st.form("login"):
        email = st.text_input("Email").strip().lower()
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")

    if submitted:
        try:
            user = get_user(email)
        except Exception:
            log.exception("Login lookup failed")
            st.error("Could not reach the database. Please try again shortly.")
        else:
            if user and verify_password(user[1], password):
                st.session_state.logged_in = True
                st.session_state.email = email
                st.session_state.user_id = user[0]
                st.rerun()
            else:
                st.error("Invalid email or password!")
    st.stop()


# ------------------------------
# Main app interface (after successful login)
# ------------------------------
st.success(f"Welcome, {st.session_state.email}!")
st.button("Log out", on_click=logout)

try:
    user_views = get_user_views(st.session_state.email)
except Exception:
    log.exception("Loading views failed")
    st.error("Error accessing available views.")
    st.stop()

if not user_views:
    st.info("No views are assigned to your account.")
    st.stop()

selected_view = st.selectbox("Select a View", list(user_views.keys()))

today = datetime.date.today()
default_end_date = today.replace(day=1) - datetime.timedelta(days=1)  # last day of previous month
default_start_date = subtract_month(default_end_date)

st.session_state.setdefault("start_date", default_start_date)
st.session_state.setdefault("end_date", default_end_date)

start_date = st.date_input(
    "Select Start Date",
    value=st.session_state.start_date,
    min_value=datetime.date(2019, 1, 1),
    max_value=st.session_state.end_date,  # start cannot be after end
)
end_date = st.date_input(
    "Select End Date",
    value=st.session_state.end_date,
    min_value=start_date,  # end cannot be before start
    max_value=today,
)
st.session_state.start_date = start_date
st.session_state.end_date = end_date

invalid_range = start_date > end_date
if invalid_range:
    st.error("Start Date cannot be later than End Date. Please select a valid range.")

# ------------------------------
# Data retrieval
# ------------------------------
if st.button("Get Data", disabled=invalid_range):
    view_id, address = user_views[selected_view]
    query = f"""
        SELECT *
        FROM {DATABASE}.{address}
        WHERE TRY_TO_DATE(AD_DATE, 'DD.MM.YYYY') BETWEEN %s AND %s
        ORDER BY TRY_TO_DATE(AD_DATE, 'DD.MM.YYYY')
    """
    st.session_state.pop("result", None)
    try:
        with st.spinner("Querying Database..."):
            t0 = time.time()
            df = run_query(query, (start_date, end_date), fetch="df")
            duration = time.time() - t0
    except Exception:
        log.exception("Data query failed (view_id=%s, %s to %s)", view_id, start_date, end_date)
        st.error("Error retrieving data.")
    else:
        st.session_state.result = {
            "df": df,
            "view": selected_view,
            "start": start_date,
            "end": end_date,
            "file": None,  # (format, bytes), built once and reused across reruns
        }
        st.success(f"Data retrieved successfully! {len(df):,} rows.")
        log_query(
            user_id=st.session_state.user_id,
            view_id=view_id,
            start_date=start_date,
            end_date=end_date,
            number_of_rows=len(df),
            query_duration=duration,
        )

# ------------------------------
# Preview and download
# ------------------------------
result = st.session_state.get("result")
if result:
    df = result["df"]
    n_rows = len(df)
    st.caption(
        f"{result['view']}, {result['start']} to {result['end']}: {n_rows:,} rows"
        + (" (preview shows the first 100)" if n_rows > 100 else "")
    )
    st.dataframe(df.head(100), width="stretch")

    if n_rows == 0:
        st.info("No rows for the selected period.")
    else:
        if n_rows > EXCEL_MAX_ROWS:
            options, note = ["CSV"], " (too many rows for Excel)"
        elif n_rows >= EXCEL_SOFT_LIMIT:
            options, note = ["Excel", "CSV"], " (generating Excel for large datasets can take time)"
        else:
            options, note = ["Excel", "CSV"], ""
        default_index = 0 if n_rows < EXCEL_SOFT_LIMIT else len(options) - 1

        fmt = st.radio(f"Select Download Format{note}", options, index=default_index, horizontal=True)

        # Build the file only when the format changes, not on every rerun
        if result["file"] is None or result["file"][0] != fmt:
            with st.spinner(f"Generating {fmt} file..."):
                try:
                    result["file"] = (fmt, build_file(df, fmt))
                except Exception:
                    log.exception("Building %s file failed", fmt)
                    st.error(f"Could not generate the {fmt} file.")

        if result["file"] and result["file"][0] == fmt:
            ext, mime = (
                ("csv", "text/csv")
                if fmt == "CSV"
                else ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            )
            st.download_button(
                label=f"Download as {fmt}",
                data=result["file"][1],
                file_name=f"{safe_filename(result['view'])}_{result['start']}_{result['end']}.{ext}",
                mime=mime,
                on_click="ignore",  # downloading does not rerun the app
            )
