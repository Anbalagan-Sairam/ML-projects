"""Create `data/nutrition.csv` from merged cleaned subset with dates split.

Output columns: Day, Month, Year, Weight, Nutrition, Exercise
Sorted by date (latest first).
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IN_FILE = ROOT / 'data' / 'merged_health_clean_subset_dates_fixed_source.csv'
OUT_FILE = ROOT / 'data' / 'nutrition.csv'

def main():
    if not IN_FILE.exists():
        print('Input file not found:', IN_FILE)
        return
    print('Loading', IN_FILE)
    df = pd.read_csv(IN_FILE, dtype=str, encoding='utf-8', low_memory=False)

    # Find date column: prefer 'Date', then 'Date_normalized', then any column containing 'date'
    date_col = None
    for candidate in ['Date','Date_normalized','date','Date_normalised']:
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col is None:
        for c in df.columns:
            if 'date' in str(c).lower():
                date_col = c
                break

    if date_col is None:
        print('No date column found; aborting')
        return

    # parse to datetime (coerce invalid)
    df['_dt'] = pd.to_datetime(df[date_col], errors='coerce')

    # create Day, Month, Year
    df['Day'] = df['_dt'].dt.day
    df['Month'] = df['_dt'].dt.month
    df['Year'] = df['_dt'].dt.year

    # keep Weight, Nutrition, Exercise if present (case-insensitive)
    cols_lower = {c.lower(): c for c in df.columns}
    weight_col = cols_lower.get('weight')
    nutrition_col = cols_lower.get('nutrition')
    exercise_col = cols_lower.get('exercise')

    out = pd.DataFrame()
    out['Day'] = df['Day']
    out['Month'] = df['Month']
    out['Year'] = df['Year']
    out['Weight'] = df[weight_col] if weight_col else None
    out['Nutrition'] = df[nutrition_col] if nutrition_col else None
    out['Exercise'] = df[exercise_col] if exercise_col else None

    # drop rows with no date (i.e., _dt is NaT)
    out['_dt'] = df['_dt']
    out = out.dropna(subset=['_dt'])

    # sort by date descending (latest first)
    out = out.sort_values('_dt', ascending=False)

    # drop helper
    out = out.drop(columns=['_dt'])

    out.to_csv(OUT_FILE, index=False)
    print('Saved', OUT_FILE, 'shape=', out.shape)

if __name__ == '__main__':
    main()
