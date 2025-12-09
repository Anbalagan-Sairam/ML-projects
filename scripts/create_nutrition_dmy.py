"""Create `data/nutrition_dmy.csv` with Date in DD-MM-YYYY, sorted newest-first.

Reads `data/nutrition.csv` (expects Day,Month,Year columns) and outputs
`nutrition_dmy.csv` where Date is a string in DD-MM-YYYY format.
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IN_FILE = ROOT / 'data' / 'nutrition.csv'
OUT_FILE = ROOT / 'data' / 'nutrition_dmy.csv'

def main():
    if not IN_FILE.exists():
        print('Input file not found:', IN_FILE)
        return
    print('Loading', IN_FILE)
    df = pd.read_csv(IN_FILE, dtype=str, encoding='utf-8', low_memory=False)

    # Ensure Day/Month/Year present
    for c in ['Day','Month','Year']:
        if c not in df.columns:
            print('Missing column', c, 'in', IN_FILE)
            return

    # Pad and build date string DD-MM-YYYY
    def to_dmy(row):
        try:
            d = int(float(row['Day']))
            m = int(float(row['Month']))
            y = int(float(row['Year']))
            return f"{d:02d}-{m:02d}-{y:04d}"
        except Exception:
            return ''

    df['Date'] = df.apply(to_dmy, axis=1)

    # parse to datetime for sorting; invalid parse -> NaT
    df['_dt'] = pd.to_datetime(df['Date'], format='%d-%m-%Y', errors='coerce')

    # sort newest-first
    df = df.sort_values('_dt', ascending=False)

    # drop helper
    df_out = df[['Date','Weight','Nutrition','Exercise']].copy()

    df_out.to_csv(OUT_FILE, index=False)
    print('Saved', OUT_FILE, 'shape=', df_out.shape)

if __name__ == '__main__':
    main()
