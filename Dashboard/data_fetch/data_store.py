# Dashboard/data_fetch/data_store.py

# Global dictionary to store the latest data
latest_data = {}

def update_data(key, data):
    """
    Update the global latest_data dictionary with the latest fetched data.
    
    Parameters:
        key (str): The key to identify the data (e.g., 'FFQ24', '2Y_Spot').
        data (any): The latest fetched data (e.g., DataFrame).
    """
    global latest_data
    latest_data[key] = data

print(latest_data)