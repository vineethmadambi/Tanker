import os
import requests
import pandas as pd
from datetime import datetime

# 1. Setup
API_KEY = os.getenv("TANKER_API_KEY")
CSV_FILE = "fuel_history.csv"

# Coordinates for Kaiserslautern
URL = f"https://creativecommons.tankerkoenig.de/json/list.php?lat=49.4401&lng=7.7491&rad=10&sort=dist&type=all&apikey={API_KEY}"

# 2. Fetch new data
response = requests.get(URL).json()

if response["ok"]:
    new_data = pd.DataFrame(response["stations"])
    new_data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Check if we already have a history file to compare against
    if os.path.exists(CSV_FILE):
        try:
            # Load current history
            history = pd.read_csv(CSV_FILE)

            # Get the very last recorded price for each unique station ID
            last_known = history.sort_values('timestamp').groupby('id').tail(1)

            # Merge new data with the last known data to compare side-by-side
            merged = new_data.merge(
                last_known[['id', 'diesel', 'e5', 'e10']],
                on='id',
                suffixes=('', '_old'),
                how='left'
            )

            # Filter: Keep only rows where price changed OR it's a new station
            changes = merged[
                (merged['diesel'] != merged['diesel_old']) |
                (merged['e5'] != merged['e5_old']) |
                (merged['e10'] != merged['e10_old']) |
                (merged['diesel_old'].isna())  # New station found
            ]

            if not changes.empty:
                to_save = changes[new_data.columns]
                to_save.to_csv(CSV_FILE, mode='a', index=False, header=False)
                print(f"Detected {len(changes)} price changes. History updated.")
            else:
                print("No price changes detected. Nothing to save.")

        except Exception as e:
            print(f"Error reading history, saving fresh: {e}")
            new_data.to_csv(CSV_FILE, mode='a', index=False, header=False)
    else:
        # First run: create the file with headers
        new_data.to_csv(CSV_FILE, mode='w', index=False, header=True)
        print("History file created for the first time.")
