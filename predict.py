import pandas as pd
import requests
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# ── Load your tennis data ────────────────────────────────────────────────────────
df = pd.read_csv('tennis_atp-master/atp_matches_2023.csv')

# ── Prepare a wind_speed column ──────────────────────────────────────────────────
df['wind_speed'] = 0.0

# ── Map tournament names to lat/lon ─────────────────────────────────────────────
# Add as many tournaments here as you want
tourney_coords = {
    'Australian Open':    (-33.847, 151.033),
    'Roland Garros':      (48.847, 2.249),    # Paris, clay
    'Wimbledon':          (51.433, -0.214),
    'US Open':            (40.749, -73.846)   # New York, hard
}

# ── Fetch wind for first 100 matches ────────────────────────────────────────────
for idx, row in df.head(100).iterrows():
    name = row['tourney_name']
    coords = tourney_coords.get(name)
    if not coords:
        # Skip if we don’t have coords for this tournament
        continue

    lat, lon = coords
    date_str = str(row['tourney_date'])
    date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    # Call Open-Meteo
    resp = requests.get(
        "https://archive-api.open-meteo.com/v1/archive",
        params={
            "latitude":      lat,
            "longitude":     lon,
            "start_date":    date_fmt,
            "end_date":      date_fmt,
            "daily":         "wind_speed_10m_max",
            "timezone":      "UTC"
        }
    ).json()
    wind = resp["daily"]["wind_speed_10m_max"][0]
    df.at[idx, 'wind_speed'] = wind
    print(f"[{idx}] {name} on {date_fmt}: {wind} m/s")

# ── Prepare features & labels ───────────────────────────────────────────────────
X = df[['winner_rank', 'loser_rank', 'wind_speed']]
y = (df['winner_rank'] < df['loser_rank']).astype(int)

# ── Split, train & evaluate ─────────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)
model = DecisionTreeClassifier(max_depth=5, random_state=42)
model.fit(X_train, y_train)
acc = accuracy_score(y_test, model.predict(X_test))

print(f'Accuracy with {len(df[df["wind_speed"]>0])} wind-filled matches: {acc:.2%}')
