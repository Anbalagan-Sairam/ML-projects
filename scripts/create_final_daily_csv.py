"""Aggregate nutrition and exercise per day into a final CSV.

Output: data/final_daily_nutrition_exercise.csv with columns:
- Date (DD-MM-YYYY)
- Food (all food items joined with ' | ')
- Exercise (all exercise items joined with ' | ')

Uses `data/nutrition_events_dmy.csv` if present, otherwise falls back to
`data/nutrition_aggregated.csv` or `data/merged_health_from_downloads_dates_fixed.csv`.
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'data' / 'final_daily_nutrition_exercise.csv'
PRIM = ROOT / 'data' / 'nutrition_events_dmy.csv'
BACK1 = ROOT / 'data' / 'nutrition_aggregated.csv'
BACK2 = ROOT / 'data' / 'merged_health_from_downloads_dates_fixed.csv'

def join_nonempty_preserve(series):
    vals = [str(x).strip() for x in series if pd.notna(x) and str(x).strip()!='']
    seen = set()
    out = []
    for v in vals:
        if v and v not in seen:
            out.append(v)
            seen.add(v)
    return ' | '.join(out)

def main():
    if PRIM.exists():
        df = pd.read_csv(PRIM, dtype=str, low_memory=False)
        date_col = 'Date'
        food_col = 'Nutrition'
        ex_col = 'Exercise'
    elif BACK1.exists():
        df = pd.read_csv(BACK1, dtype=str, low_memory=False)
        # assume aggregated already has Nutrition and Exercise joined
        date_col = 'Date'
        food_col = 'Nutrition'
        ex_col = 'Exercise'
    elif BACK2.exists():
        df = pd.read_csv(BACK2, dtype=str, low_memory=False)
        # try to locate columns
        cols = {c.lower(): c for c in df.columns}
        date_col = cols.get('date_normalized') or cols.get('date')
        # guess food/ex columns
        food_col = cols.get('nutrition') or cols.get('food')
        ex_col = cols.get('exercise')
    else:
        print('No input data found to aggregate')
        return

    # normalize date parsing: expect DD-MM-YYYY in PRIM, else parse
    # create a parseable datetime column
    if PRIM.exists():
        # PRIM Date format is DD-MM-YYYY
        df['_dt'] = pd.to_datetime(df[date_col], format='%d-%m-%Y', errors='coerce')
    else:
        df['_dt'] = pd.to_datetime(df[date_col], errors='coerce')

    # group by date (use formatted DD-MM-YYYY for final)
    df['Date_DMY'] = df['_dt'].dt.strftime('%d-%m-%Y')

    # aggregate
    agg = df.groupby('Date_DMY').agg({
        food_col: join_nonempty_preserve if food_col in df.columns else (lambda s: ''),
        ex_col: join_nonempty_preserve if ex_col in df.columns else (lambda s: ''),
    }).reset_index()

    # rename to requested columns
    agg = agg.rename(columns={'Date_DMY': 'Date', food_col: 'Food', ex_col: 'Exercise'})

    # sort newest-first
    agg['_dt'] = pd.to_datetime(agg['Date'], format='%d-%m-%Y', errors='coerce')
    agg = agg.sort_values('_dt', ascending=False).drop(columns=['_dt'])

    agg.to_csv(OUT, index=False)
    print('Saved final CSV to', OUT, 'shape=', agg.shape)

if __name__ == '__main__':
    main()
