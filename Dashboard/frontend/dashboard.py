import streamlit as st
import pandas as pd
import os

# Directory where the CSV files are stored
CSV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../data_fetch/csvfiles')

def load_latest_data_from_csv(filename):
    """
    Load the last row of data from a CSV file, handling parsing errors.
    
    Parameters:
        filename (str): The name of the CSV file to load.
    
    Returns:
        DataFrame: The last row of data as a pandas DataFrame.
    """
    file_path = os.path.join(CSV_DIR, f"{filename}.csv")
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, sep=',', on_bad_lines='warn')  # Use on_bad_lines only
            if not df.empty:
                return df.tail(1)  # Get the last row
        except pd.errors.ParserError as e:
            st.error(f"Error parsing {filename}.csv: {e}")
    else:
        st.warning(f"{filename}.csv not found or empty.")
    return pd.DataFrame()

# Streamlit Dashboard
st.title("Financial Market Dashboard")

# Fed Fund Futures Contracts
# Define the correct order of Fed Fund Futures Contracts
contract_order = ["FFQ24", "FFU24", "FFV24", "FFX24", "FFZ24", "FFF25", "FFG25", "FFH25", "FFM25", "FFK25", "FFN25"]

# Fed Fund Futures Contracts
st.subheader("Fed Fund Futures Contracts")
fed_fund_data = pd.DataFrame()  # Initialize an empty DataFrame
for contract in contract_order:
    data = load_latest_data_from_csv(contract)
    if not data.empty:
        data['Contract'] = contract  # Add a column for the contract name
        fed_fund_data = pd.concat([fed_fund_data, data], ignore_index=True)  # Append data to the main DataFrame

if not fed_fund_data.empty:
    fed_fund_data['Contract'] = pd.Categorical(fed_fund_data['Contract'], categories=contract_order, ordered=True)
    fed_fund_data = fed_fund_data.sort_values('Contract')  # Sort the DataFrame by the contract order
    st.dataframe(fed_fund_data)
    
    # Assuming there's a column named 'Price' in the fed_fund_data
    st.line_chart(fed_fund_data.set_index('Contract')['BID'])

# Implied Rate Probabilities with Meeting Dates
st.subheader("Implied Rate Probabilities")
rate_prob_data = pd.DataFrame()  # Initialize an empty DataFrame
for meeting in ["FOMC_Sep24", "FOMC_Nov24", "FOMC_Dec24", "FOMC_Jan25", "FOMC_Mar25", "FOMC_May25", "FOMC_Jun25", "FOMC_Jul25", "FOMC_Sep25"]:
    data = load_latest_data_from_csv(meeting)
    if not data.empty:
        data['Meeting'] = meeting  # Add a column for the meeting name
        rate_prob_data = pd.concat([rate_prob_data, data], ignore_index=True)  # Append data to the main DataFrame

if not rate_prob_data.empty:
    st.dataframe(rate_prob_data)

# Treasury Spots and Yields
st.subheader("Treasury Spots and Yields")
treasury_data = pd.DataFrame()  # Initialize an empty DataFrame
for spot in ["2Y_Spot", "5Y_Spot", "10Y_Spot"]:
    data = load_latest_data_from_csv(spot)
    if not data.empty:
        data['Term'] = spot  # Add a column for the term (2Y, 5Y, 10Y)
        treasury_data = pd.concat([treasury_data, data], ignore_index=True)  # Append data to the main DataFrame

if not treasury_data.empty:
    st.dataframe(treasury_data)

# VOLC Matrix
st.subheader("VOLC Matrix for SRAZ24")
volc_matrix = load_latest_data_from_csv("sraz24_volc_matrix")
if not volc_matrix.empty:
    volcmainlist = volc_matrix.iloc[0]
    print(volc_matrix.head)
    volcmainlist = [item for item in volcmainlist if item is not None]
    st.dataframe(volcmainlist)
