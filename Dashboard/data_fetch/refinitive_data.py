import refinitiv.data as rd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Instrument Identifiers (Tickers) and Field Names
INSTRUMENTS = {
    "2Y_Spot": {"ticker": "US2YT=RR", "fields": ['BID', 'ASK','RT_YIELD_1']},
    "5Y_Spot": {"ticker": "US5YT=RR", "fields": ['BID', 'ASK','RT_YIELD_1']},
    "10Y_Spot": {"ticker": "US10YT=RR", "fields": ['BID', 'ASK','RT_YIELD_1']},
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

def main():
    """
    Main function to fetch US Treasury Spot Prices, Yields, FED Fund Futures Quotes, and FOMC Implied Rates.
    """
    start_session()
    
    results = {}
    for key, instrument in INSTRUMENTS.items():
        ticker = instrument["ticker"]
        fields = instrument["fields"]
        data = fetch_data(ticker, fields)
        if data is not None:
            results[key] = data
    
    close_session()
    
    logging.info("Final fetched data:")
    for key, data in results.items():
        logging.info(f"{key}: \n{data}")

    return results

if __name__ == "__main__":
    main()
