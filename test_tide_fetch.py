import requests
from datetime import datetime, timedelta

COOPS_BASE_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
station_id = "9412110"  # Los Angeles
begin_date = "20251023"
end_date = "20251023"

params = {
    "product": "predictions",
    "station": station_id,
    "datum": "MLLW",
    "units": "english",
    "time_zone": "lst_ldt",
    "format": "json",
    "begin_date": begin_date,
    "end_date": end_date,
    "interval": "6"
}

response = requests.get(COOPS_BASE_URL, params=params, timeout=30)
data = response.json()
predictions = data.get("predictions", [])

print(f"Total predictions: {len(predictions)}")

# Filter to 15-minute intervals
filtered = []
for pred in predictions:
    timestamp_str = pred.get('t', '')
    if timestamp_str:
        try:
            minute = int(timestamp_str.split(':')[1].split()[0])
            if minute % 15 == 0:
                filtered.append(pred)
        except (ValueError, IndexError):
            continue

print(f"Filtered to 15-min: {len(filtered)}")
print("\nFirst 10 filtered timestamps:")
for p in filtered[:10]:
    print(f"  {p['t']} - {p['v']} ft")
