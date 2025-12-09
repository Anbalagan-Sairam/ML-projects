"""Produce two nutrition outputs:
- `nutrition_full_rows.csv`: all source rows with normalized dates preserved (no collapsing)
- `nutrition_aggregated.csv`: one row per date with all Nutrition and Exercise entries joined

Reads `data/merged_health_from_downloads_dates_fixed.csv` (contains `Date_normalized`).
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IN_FILE = ROOT / 'data' / 'merged_health_from_downloads_dates_fixed.csv'
OUT_FULL = ROOT / 'data' / 'nutrition_full_rows.csv'
OUT_AGG = ROOT / 'data' / 'nutrition_aggregated.csv'

def safe_str(x):
    if pd.isna(x):
        return ''
    s = str(x).strip()
    return s

def main():
    if not IN_FILE.exists():
        print('Input not found:', IN_FILE)
        return
    print('Loading', IN_FILE)
    df = pd.read_csv(IN_FILE, dtype=str, low_memory=False)

    # prefer Date_normalized if present
    date_col = 'Date_normalized' if 'Date_normalized' in df.columns else ('Date' if 'Date' in df.columns else None)
    if date_col is None:
        print('No date column found')
        return

    # Ensure we have Nutrition and Exercise columns or find approximations
    cols = {c.lower(): c for c in df.columns}
    nutrition_col = cols.get('nutrition') or None
    exercise_col = cols.get('exercise') or None
    weight_col = cols.get('weight') or None

    # Full rows: keep Date (normalized), Weight, Nutrition, Exercise, source_file, source_sheet
    full = pd.DataFrame()
    full['Date'] = df[date_col]
    full['Weight'] = df[weight_col] if weight_col else ''
    full['Nutrition'] = df[nutrition_col] if nutrition_col else ''
    full['Exercise'] = df[exercise_col] if exercise_col else ''
    if 'source_file' in df.columns:
        full['source_file'] = df['source_file']
    if 'source_sheet' in df.columns:
        full['source_sheet'] = df['source_sheet']

    # drop rows without a parsed date
    full = full[full['Date'].notna() & (full['Date'].astype(str) != '')]
    # save full rows
    full.to_csv(OUT_FULL, index=False)
    print('Wrote full rows to', OUT_FULL, 'shape=', full.shape)

    # Aggregated: group by Date, join non-empty Nutrition and Exercise entries preserving order
    def join_nonempty(series):
        vals = [safe_str(x) for x in series if str(x).strip() not in ['', 'nan', 'None']]
        # keep original order and deduplicate while preserving order
        seen = set()
        out = []
        for v in vals:
            if v and v not in seen:
                out.append(v)
                seen.add(v)
        return ' | '.join(out)

    agg = full.groupby('Date').agg({
        'Weight': lambda s: next((x for x in s[::-1] if str(x).strip() not in ['', 'nan', 'None']), ''),
        'Nutrition': join_nonempty,
        'Exercise': join_nonempty,
    }).reset_index()

    # sort by date descending (try parse)
    agg['_dt'] = pd.to_datetime(agg['Date'], errors='coerce')
    agg = agg.sort_values('_dt', ascending=False).drop(columns=['_dt'])

    agg.to_csv(OUT_AGG, index=False)
    print('Wrote aggregated file to', OUT_AGG, 'shape=', agg.shape)

if __name__ == '__main__':
    main()
