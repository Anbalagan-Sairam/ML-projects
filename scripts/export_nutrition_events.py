"""Export every nutrition/exercise event row with Date in DD-MM-YYYY.

Reads `data/merged_health_from_downloads_dates_fixed.csv` (uses `Date_normalized`)
and writes `data/nutrition_events_dmy.csv` containing one row per source row
where `Nutrition` or `Exercise` is present. Date is formatted as DD-MM-YYYY.
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IN_FILE = ROOT / 'data' / 'merged_health_from_downloads_dates_fixed.csv'
OUT_FILE = ROOT / 'data' / 'nutrition_events_dmy.csv'

def main():
    if not IN_FILE.exists():
        print('Input not found:', IN_FILE)
        return
    print('Loading', IN_FILE)
    df = pd.read_csv(IN_FILE, dtype=str, low_memory=False)

    # pick normalized date
    date_col = 'Date_normalized' if 'Date_normalized' in df.columns else ('Date' if 'Date' in df.columns else None)
    if date_col is None:
        print('No date column found')
        return

    # find nutrition/exercise columns (case-insensitive)
    cols_lower = {c.lower(): c for c in df.columns}
    nutrition_col = cols_lower.get('nutrition')
    exercise_col = cols_lower.get('exercise')
    weight_col = cols_lower.get('weight')

    # create events df keeping rows that have nutrition or exercise
    def nonempty(val):
        return False if pd.isna(val) else str(val).strip() != ''

    mask = False
    if nutrition_col:
        mask = mask | df[nutrition_col].notna() & (df[nutrition_col].astype(str).str.strip() != '')
    if exercise_col:
        mask = mask | df[exercise_col].notna() & (df[exercise_col].astype(str).str.strip() != '')
    # if neither column found, look for any column with word 'food' or similar
    if not (nutrition_col or exercise_col):
        # fallback: include rows that have any non-empty cell besides source columns
        mask = df.apply(lambda r: any(str(x).strip() for x in r if x and 'source' not in str(x).lower()), axis=1)

    events = df[mask].copy()

    # parse date and format DMY
    events['_dt'] = pd.to_datetime(events[date_col], errors='coerce')
    events['Date'] = events['_dt'].dt.strftime('%d-%m-%Y')

    # build output columns
    out = pd.DataFrame()
    out['Date'] = events['Date']
    out['Weight'] = events[weight_col] if weight_col else None
    out['Nutrition'] = events[nutrition_col] if nutrition_col else None
    out['Exercise'] = events[exercise_col] if exercise_col else None
    if 'source_file' in events.columns:
        out['source_file'] = events['source_file']
    if 'source_sheet' in events.columns:
        out['source_sheet'] = events['source_sheet']

    # keep order as in file; do not aggregate or dedupe
    out.to_csv(OUT_FILE, index=False)
    print('Saved events to', OUT_FILE, 'shape=', out.shape)

if __name__ == '__main__':
    main()
