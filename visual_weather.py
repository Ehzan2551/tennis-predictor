import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('VISUAL_CROSSING_API_KEY')

BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
TOURNAMENTS = {
    'Wimbledon': '51.433,-0.214',
    'Roland Garros': '48.847,2.249',
    'Australian Open': '-33.847,151.033',
    'US Open': '40.749,-73.846'
}
YEARS = list(range(2018, 2025))  # 2018-2024
ELEMENTS = 'datetime,wspd,temp,precip,humidity'

all_dfs = []
for name, loc in TOURNAMENTS.items():
    for year in YEARS:
        start = f"{year}-01-01"
        end = f"{year}-12-31"
        url = (
            f"{BASE_URL}/{loc}/{start}/{end}"
            f"?unitGroup=metric"
            f"&elements={ELEMENTS}"
            f"&include=days"
            f"&key={API_KEY}"
            f"&contentType=csv"
        )
        print(f"Fetching {name} {year}…")
        try:
            df = pd.read_csv(url)
            df['tourney_name'] = name
            all_dfs.append(df)
        except Exception as e:
            print(f"Failed for {name} {year}: {e}")

weather_df = pd.concat(all_dfs, ignore_index=True)
weather_df.to_csv('vc_historical_weather.csv', index=False)
print(f"Saved {len(weather_df)} rows to vc_historical_weather.csv")
