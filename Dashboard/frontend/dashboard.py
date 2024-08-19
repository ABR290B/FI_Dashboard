import streamlit as st
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ..data_fetch.data_store import latest_data  # Import the variable directly

# Streamlit Dashboard

st.title("Financial Market Dashboard")

# Fed Fund Futures Contracts
st.subheader("Fed Fund Futures Contracts")
for contract in ["FFQ24", "FFU24", "FFV24", "FFX24", "FFZ24", "FFH25", "FFM25", "FFK25", "FFN25"]:
    data = latest_data.get(contract)
    if data is not None:
        st.write(f"### {contract}")
        st.dataframe(data)

# Implied Rate Probabilities with Meeting Dates
st.subheader("Implied Rate Probabilities")
for meeting in ["FOMC_Sep24", "FOMC_Nov24", "FOMC_Dec24", "FOMC_Jan25", "FOMC_Mar25", "FOMC_May25", "FOMC_Jun25", "FOMC_Jul25", "FOMC_Sep25"]:
    data = latest_data.get(meeting)
    if data is not None:
        st.write(f"### {meeting}")
        st.dataframe(data)

# Treasury Spots and Yields
st.subheader("Treasury Spots and Yields")
for spot in ["2Y_Spot", "5Y_Spot", "10Y_Spot"]:
    data = latest_data.get(spot)
    if data is not None:
        st.write(f"### {spot}")
        st.dataframe(data)

# VOLC Matrix
st.subheader("VOLC Matrix for SRAZ24")
volc_matrix = latest_data.get("SRAZ24_VOLC")
if volc_matrix is not None:
    st.dataframe(volc_matrix)
