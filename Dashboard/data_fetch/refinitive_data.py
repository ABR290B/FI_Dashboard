import refinitiv.data as rd
import logging
from refinitiv.data.content.ipa import surfaces
import time
import pandas as pd
import datetime
import os
from data_store import update_data  # Import the function to update data

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Instrument Identifiers (Tickers) and Field Names
INSTRUMENTS = {
    "2Y_Spot": {"ticker": "US2YT=RR", "fields": ['BID', 'ASK','RT_YIELD_1']},
    "5Y_Spot": {"ticker": "US5YT=RR", "fields": ['BID', 'ASK','RT_YIELD_1']},
    "10Y_Spot": {"ticker": "US10YT=RR", "fields": ['BID', 'ASK','RT_YIELD_1']},
    # FED FUND O/Rs
    "FFQ24": {"ticker": "FFQ24", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFU24": {"ticker": "FFU24", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFV24": {"ticker": "FFV24", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFX24": {"ticker": "FFX24", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFZ24": {"ticker": "FFZ24", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFF25": {"ticker": "FFF25", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFG25": {"ticker": "FFG25", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFH25": {"ticker": "FFH25", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFM25": {"ticker": "FFM25", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFK25": {"ticker": "FFK25", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    "FFN25": {"ticker": "FFN25", "fields": ['BID', 'ASK', 'TRDPRC_1', 'OPEN_PRC', 'CLOSE_PRC']},
    # Implied Rates in Basis Points for FOMC Meetings
    "FOMC_Sep24": {"ticker": "USIRP25F1=R", "fields": ['IMPLD_BPS']},
    "FOMC_Nov24": {"ticker": "USIRP25F2=R", "fields": ['IMPLD_BPS']},
    "FOMC_Dec24": {"ticker": "USIRP25F3=R", "fields": ['IMPLD_BPS']},
    "FOMC_Jan25": {"ticker": "USIRP25F4=R", "fields": ['IMPLD_BPS']},
    "FOMC_Mar25": {"ticker": "USIRP25F5=R", "fields": ['IMPLD_BPS']},
    "FOMC_May25": {"ticker": "USIRP25F6=R", "fields": ['IMPLD_BPS']},
    "FOMC_Jun25": {"ticker": "USIRP25F7=R", "fields": ['IMPLD_BPS']},
    "FOMC_Jul25": {"ticker": "USIRP25F8=R", "fields": ['IMPLD_BPS']},
    "FOMC_Sep25": {"ticker": "USIRP25F9=R", "fields": ['IMPLD_BPS']},
    "SRAZ24_VOLC": {"ticker": "SRAZ24"}
}

def start_session():
    """
    Starts a session with the Refinitiv Data Platform.
    """
    try:
        logging.info("Starting Refinitiv session...")
        rd.open_session()
        logging.info("Session started successfully.")
    except Exception as e:
        logging.error(f"Failed to start session: {e}")
        exit()

def close_session():
    """
    Closes the session with the Refinitiv Data Platform.
    """
    try:
        rd.close_session()
        logging.info("Session closed successfully.")
    except Exception as e:
        logging.error(f"Failed to close session: {e}")

def fetch_volc_matrix_for_sraz24():
    """
    Fetches the VOLC Matrix for the SRAZ24 contract.
    
    Returns:
        DataFrame: The fetched VOLC Matrix as a pandas DataFrame.
    """
    try:
        logging.info(f"Fetching VOLC Matrix for SRAZ24")
        response = rd.content.ipa.surfaces.eti.Definition(
            surface_tag="SRAZ24_Volatility_Surface",
            underlying_definition={
                "instrumentCode": "SRAZ24"
            },
            surface_parameters={
            },
            surface_layout={
                "format": "Matrix"
            }
        ).get_data()

        if response is not None and not response.data.df.empty:
            logging.info(f"VOLC Matrix fetched successfully for SRAZ24")
            return response.data.df
        else:
            logging.warning(f"No VOLC Matrix found for SRAZ24.")
            return None
    except Exception as e:
        logging.error(f"Error fetching VOLC Matrix for SRAZ24: {e}")
        return None

def fetch_data(ticker, fields):
    """
    Fetches data for a specific instrument.
    
    Parameters:
        ticker (str): The instrument identifier (ticker).
        fields (list): The fields specifying the type of data to retrieve.
    
    Returns:
        DataFrame: The fetched data as a pandas DataFrame.
    """
    try:
        logging.info(f"Fetching data for ticker: {ticker}, fields: {fields}")
        data = rd.get_data(universe=[ticker], fields=fields)
        if data is not None and not data.empty:
            logging.info(f"Data fetched successfully for {ticker}")
            return data
        else:
            logging.warning(f"No data found for {ticker}.")
            return None
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return None

def fetch_prices_yields_implied_rates():
    """
    Fetches Prices, Yields, and Implied Rates (excluding VOLC).
    """
    results = {}
    for key, instrument in INSTRUMENTS.items():
        if key != "SRAZ24_VOLC":  # Skip VOLC as it doesn't need to be updated frequently
            ticker = instrument["ticker"]
            fields = instrument.get("fields", [])
            data = fetch_data(ticker, fields)
            if data is not None:
                results[key] = data
    return results

# Example function to save fetched data
def append_data_to_csv(data, filename):
    """
    Append fetched data to a CSV file with a timestamp.
    
    Parameters:
        data (DataFrame): The data to save.
        filename (str): The name of the CSV file to save the data in.
    """
    if data is not None:
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data.insert(0, 'Timestamp', timestamp)
        
        file_path = f'Dashboard/data_fetch/csvfiles/{filename}.csv'
        
        if not os.path.exists(file_path):
            data.to_csv(file_path, index=False)
            logging.info(f"Data saved to {filename}.csv")
        else:
            data.to_csv(file_path, mode='a', header=False, index=False)
            logging.info(f"Data appended to {filename}.csv")
    else:
        logging.warning(f"No data to save for {filename}")

def main():
    start_session()
    
    # Fetch VOLC Matrix once and store it
    volc_matrix = fetch_volc_matrix_for_sraz24()
    append_data_to_csv(volc_matrix, 'sraz24_volc_matrix')
    update_data('SRAZ24_VOLC', volc_matrix)
    
    while True:
        results = fetch_prices_yields_implied_rates()
        
        # Append each fetched DataFrame to the corresponding CSV file and update the latest data
        for key, data in results.items():
            append_data_to_csv(data, key)
            update_data(key, data)
        
        time.sleep(60)
    
    close_session()

if __name__ == "__main__":
    main()