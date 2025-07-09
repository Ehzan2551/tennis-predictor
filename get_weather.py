import os, glob, csv, pandas as pd, requests_cache, requests
from geopy.geocoders import Nominatim

# — CONFIG —  
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))  
CACHE_FILE = os.path.join(BASE_DIR, 'weather_cache.csv')  
INPUT_GLOB = os.path.join(BASE_DIR, 'tennis_atp-master', 'atp_matches_*.csv')  
OUTPUT_DIR = os.path.join(BASE_DIR, 'enriched_data')  
START_COLS = [  
    'latitude','longitude','date',  
    'temperature_max','temperature_min',  
    'precipitation_sum','wind_speed_max'  
]

# 1) Load or initialize cache  
if not os.path.exists(CACHE_FILE):  
    with open(CACHE_FILE, 'w', newline='') as f:  
        csv.writer(f).writerow(START_COLS)  

try:  
    cache_df = pd.read_csv(CACHE_FILE, parse_dates=['date'])  
except (pd.errors.EmptyDataError, pd.errors.ParserError):  
    cache_df = pd.DataFrame(columns=START_COLS)

# 2) HTTP session with caching  
session = requests_cache.CachedSession('.http_cache', expire_after=-1)

# 3) Static coords for major tournaments  
tourney_coords = {  
    'Australian Open': (-37.8201, 144.9787),  
    'Roland Garros':   (48.8470,   2.2510),  
    'Wimbledon':       (51.4338,  -0.2142),  
    'US Open':         (40.7498,  -73.8447),  
}

# 4) Geocoding fallback  
geolocator = Nominatim(user_agent='tennis-weather', timeout=10)  
def geocode(name):  
    loc = geolocator.geocode(f"{name} tennis tournament")  
    return (loc.latitude, loc.longitude) if loc else None  

# 5) Fetch & cache one day’s weather  
def fetch_weather(lat, lon, date_str):  
    date_val = pd.to_datetime(date_str)  
    hit = cache_df[(cache_df.latitude==lat)&(cache_df.longitude==lon)&(cache_df.date==date_val)]  
    if not hit.empty:  
        return hit.iloc[0].to_dict()  

    # otherwise call API  
    url    = 'https://archive-api.open-meteo.com/v1/archive'  
    params = {  
        'latitude':   lat,  
        'longitude':  lon,  
        'start_date': date_str,  
        'end_date':   date_str,  
        'daily':      'temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max',  
        'timezone':   'UTC'  
    }  
    resp = session.get(url, params=params)  
    resp.raise_for_status()  
    d = resp.json()['daily']  

    row = {  
        'latitude':         lat,  
        'longitude':        lon,  
        'date':             date_val,  
        'temperature_max':  d['temperature_2m_max'][0],  
        'temperature_min':  d['temperature_2m_min'][0],  
        'precipitation_sum':d['precipitation_sum'][0],  
        'wind_speed_max':   d['windspeed_10m_max'][0]  
    }  

    cache_df.loc[len(cache_df)] = row  
    cache_df.to_csv(CACHE_FILE, index=False)  
    return row  

# 6) Enrich all match files  
os.makedirs(OUTPUT_DIR, exist_ok=True)  
for csv_file in glob.glob(INPUT_GLOB):  
    df = pd.read_csv(csv_file, parse_dates=['tourney_date'])  

    # init weather columns  
    for col in START_COLS[3:]:  
        df[col] = None  

    # fetch & assign weather  
    for idx, match in df.iterrows():  
        name     = match['tourney_name']  
        date_str = match['tourney_date'].strftime('%Y-%m-%d')  

        coords = tourney_coords.get(name) or geocode(name)  
        if not coords:  
            continue  
        tourney_coords[name] = coords  
        lat, lon = coords  

        weather = fetch_weather(lat, lon, date_str)  
        df.at[idx, 'temperature_max']   = weather['temperature_max']  
        df.at[idx, 'temperature_min']   = weather['temperature_min']  
        df.at[idx, 'precipitation_sum'] = weather['precipitation_sum']  
        df.at[idx, 'wind_speed_max']    = weather['wind_speed_max']  

    out_path = os.path.join(OUTPUT_DIR, os.path.basename(csv_file).replace('.csv','_weather.csv'))  
    df.to_csv(out_path, index=False)  
    print(f"→ wrote {out_path}")  

print("Done—all files enriched.")  
