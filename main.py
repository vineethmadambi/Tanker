import os
import requests
import pandas as pd
from datetime import datetime

# 1. Setup
API_KEY = os.getenv("TANKER_API_KEY")
CSV_FILE = "fuel_history.csv"

# Cities to track: name, lat, lng, radius (km)
CITIES = [
    {"name": "Kaiserslautern", "lat": 49.4401, "lng": 7.7491,  "rad": 10},
    {"name": "Neustadt",       "lat": 49.3517, "lng": 8.1381,  "rad": 10},
    {"name": "Mannheim",       "lat": 49.4875, "lng": 8.4660,  "rad": 10},
]

# 2. Check if we should save this run (only once per hour)
now = datetime.now()
should_save = False

if os.path.exists(CSV_FILE):
    try:
        history = pd.read_csv(CSV_FILE)
        last_timestamp = pd.to_datetime(history['timestamp']).max()
        minutes_since_last_save = (now - last_timestamp).total_seconds() / 60
        should_save = minutes_since_last_save >= 60
        print(f"Last save was {int(minutes_since_last_save)} minutes ago. Saving: {should_save}")
    except Exception as e:
        print(f"Could not read history file: {e}. Will save fresh.")
        should_save = True
else:
    should_save = True  # First run, always save
    print("No history file found. This is the first run.")

# 3. Fetch data for all cities
all_data = []

for city in CITIES:
    URL = (
        f"https://creativecommons.tankerkoenig.de/json/list.php"
        f"?lat={city['lat']}&lng={city['lng']}&rad={city['rad']}"
        f"&sort=dist&type=all&apikey={API_KEY}"
    )
    try:
        response = requests.get(URL, timeout=10).json()
        if response.get("ok"):
            df = pd.DataFrame(response["stations"])
            df['city'] = city['name']          # Add city column
            df['timestamp'] = now.strftime("%Y-%m-%d %H:%M:%S")
            all_data.append(df)
            print(f"✓ {city['name']}: {len(df)} stations fetched")
        else:
            print(f"✗ {city['name']}: API returned not ok")
    except Exception as e:
        print(f"✗ {city['name']}: Error fetching data – {e}")

if not all_data:
    print("No data fetched at all. Exiting.")
    exit()

# 4. Combine all cities into one DataFrame
new_data = pd.concat(all_data, ignore_index=True)

# Always print current prices to the Actions log
print("\n===== CURRENT PRICES =====")
cols_to_show = ['city', 'name', 'brand', 'diesel', 'e5', 'e10', 'dist']
available_cols = [c for c in cols_to_show if c in new_data.columns]
print(new_data[available_cols].to_string(index=False))
print("==========================\n")

# 5. Save to CSV if one hour has passed (or first run)
if should_save:
    if os.path.exists(CSV_FILE):
        new_data.to_csv(CSV_FILE, mode='a', index=False, header=False)
    else:
        new_data.to_csv(CSV_FILE, mode='w', index=False, header=True)
    print(f"Saved {len(new_data)} rows to history at {now.strftime('%Y-%m-%d %H:%M:%S')}")
else:
    print("Skipped saving — less than 60 minutes since last save.")
