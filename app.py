import streamlit as st
import pandas as pd
from db import get_engine
import queries

# --------------------------------------------------
# DB ENGINE
# --------------------------------------------------
engine = get_engine()

# --------------------------------------------------
# STREAMLIT CONFIG
# --------------------------------------------------
st.set_page_config(page_title="AMI ANALYTICS", layout="wide")
st.title("⚡ AMI BIHAR Analytics")

# --------------------------------------------------
# SIDEBAR MENU
# --------------------------------------------------
menu = st.sidebar.selectbox(
    "Select Report",
    [
        "MDMS Stage Counts",
        "MDMS BLP Vendor Counts",
        "MDMS DLP Vendor Counts",
        "HES Profile Counts",
        "SPM Billing",
        "DB Size",
        "NFMS LOAD PROFILE",
        "NFMS ENERGY DATA",
        "NFMS EVENTS PROFILE"
    ]
)

# --------------------------------------------------
# DATE INPUTS
# --------------------------------------------------
start = st.date_input("Start Date")
end = st.date_input("End Date")

# Common timestamp params
common_params = {
    "start": f"{start} 00:00:00",
    "end": f"{end} 23:59:59",
}

# Integer YYYYMMDD (for MDMS/NFMS schemas)
from_date_int = int(start.strftime("%Y%m%d"))
to_date_int = int(end.strftime("%Y%m%d"))

# --------------------------------------------------
# RUN BUTTON
# --------------------------------------------------
if st.button("Run Report"):

    # -------------------------------
    # MDMS STAGE COUNTS
    # -------------------------------
    if menu == "MDMS Stage Counts":
        df = pd.read_sql(
            queries.MDMS_STAGE_COUNTS,
            engine,
            params=common_params
        )
        st.dataframe(df, use_container_width=True)

    # -------------------------------
    # MDMS BLP VENDOR
    # -------------------------------
    elif menu == "MDMS BLP Vendor Counts":
        df = pd.read_sql(
            queries.MDMS_BLP_VENDOR,
            engine,
            params={"dt": from_date_int}
        )
        st.dataframe(df, use_container_width=True)

    # -------------------------------
    # MDMS DLP VENDOR
    # -------------------------------
    elif menu == "MDMS DLP Vendor Counts":
        df = pd.read_sql(
            queries.MDMS_DLP_VENDOR,
            engine,
            params={"dt": from_date_int}
        )
        st.dataframe(df, use_container_width=True)

    # -------------------------------
    # HES PROFILE COUNTS
    # -------------------------------
    elif menu == "HES Profile Counts":
        df = pd.read_sql(
            queries.HES_PROFILE_COUNTS,
            engine,
            params={
                "dt": start,
                "start": f"{start} 00:00:00",
                "end": f"{start} 23:59:59"
            }
        )
        st.dataframe(df, use_container_width=True)

    # -------------------------------
    # DB SIZE
    # -------------------------------
    elif menu == "DB Size":
        df = pd.read_sql(queries.DB_SIZE, engine)
        st.dataframe(df, use_container_width=True)

    # -------------------------------
    # SPM BILLING
    # -------------------------------
    elif menu == "SPM Billing":
        df = pd.read_sql(
            queries.SPM_BILLING,
            engine,
            params={
                "from_date": start,
                "to_date": end
            }
        )
    # -------------------------------
    # NFMS LOAD PROFILE
    # -------------------------------
    elif menu == "NFMS LOAD PROFILE":
        df = pd.read_sql(
            queries.nfms_LP,
            engine,
            params={
                "from_date": from_date_int,
                "to_date": to_date_int
            }
        )
        st.dataframe(df, use_container_width=True)

    # -------------------------------
    # NFMS ENERGY DATA
    # -------------------------------
    elif menu == "NFMS ENERGY DATA":
        df = pd.read_sql(
            queries.nfms_ED,
            engine,
            params={
                "from_date": from_date_int,
                "to_date": to_date_int
            }
        )
        st.dataframe(df, use_container_width=True)

    # -------------------------------
    # NFMS EVENTS PROFILE
    # -------------------------------
    elif menu == "NFMS EVENTS PROFILE":
        df = pd.read_sql(
            queries.nfms_EVENTS,
            engine,
            params={
                "from_date": from_date_int,
                "to_date": to_date_int
            }
        )
        st.dataframe(df, use_container_width=True)
