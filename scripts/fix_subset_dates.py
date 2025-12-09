"""Normalize `Date` column in merged_health_clean_subset.csv to ISO YYYY-MM-DD.

Converts Excel serial numbers (e.g., 45924) to dates using Excel epoch
and attempts parsing for other textual dates. Saves a fixed CSV and a
report of rows where date could not be parsed.
"""

import pandas as pd
from pathlib import Path
from dateutil.parser import parse
import re

ROOT = Path(__file__).resolve().parents[1]
IN_FILE = ROOT / 'data' / 'merged_health_clean_subset.csv'
OUT_FILE = ROOT / 'data' / 'merged_health_clean_subset_dates_fixed.csv'
BAD_FILE = ROOT / 'data' / 'bad_date_rows_from_subset.csv'

excel_epoch = pd.Timestamp('1899-12-30')

def parse_date_value(v):
    # handle NaN/None
    if pd.isna(v):
        return pd.NaT
    s = str(v).strip()
    if s == '' or s.lower() in ['nan','none','na']:
        return pd.NaT
    # if looks like a float with .0 -> strip
    m = re.fullmatch(r"(\d+)\.0+", s)
    if m:
        s = m.group(1)
    # if purely numeric -> possible Excel serial or YYYYMMDD
    if re.fullmatch(r"\d+", s):
        try:
            n = int(s)
            # treat as excel serial when > 29500 (approx 1980+)
            if n > 29500:
                try:
                    return excel_epoch + pd.to_timedelta(n, unit='D')
                except Exception:
                    pass
            # maybe YYYYMMDD
            if len(s) == 8:
                try:
                    return pd.to_datetime(s, format='%Y%m%d')
                except Exception:
                    pass
            if len(s) == 6:
                try:
                    return pd.to_datetime(s, format='%y%m%d')
                except Exception:
                    pass
        except Exception:
            pass
    # try common formats and dateutil
    for fmt in ['%d/%m/%Y','%d-%m-%Y','%Y-%m-%d','%m/%d/%Y','%d %b %Y','%d %B %Y','%Y.%m.%d']:
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            pass
    try:
        return parse(s, dayfirst=False, yearfirst=False)
    except Exception:
        try:
            return parse(s, dayfirst=True, yearfirst=False)
        except Exception:
            return pd.NaT

def main():
    if not IN_FILE.exists():
        print('Input file not found:', IN_FILE)
        return
    print('Loading', IN_FILE)
    df = pd.read_csv(IN_FILE, dtype=str, encoding='utf-8', low_memory=False)
    if 'Date' not in df.columns:
        print('No `Date` column found in', IN_FILE)
        return

    df['Date_parsed'] = df['Date'].apply(parse_date_value)
    # coerce to datetime and format
    df['Date_fixed'] = pd.to_datetime(df['Date_parsed'], errors='coerce')
    df['Date_fixed'] = df['Date_fixed'].dt.strftime('%Y-%m-%d')

    # rows where Date_fixed is NA or 'NaT'
    bad = df[df['Date_fixed'].isna()].copy()
    if not bad.empty:
        print('Found', len(bad), 'rows with unparseable dates; saving to', BAD_FILE)
        bad.to_csv(BAD_FILE, index=False)
    else:
        print('All dates parsed successfully')

    # replace Date with Date_fixed, keep Date_raw for traceability
    df['Date_raw'] = df['Date']
    df['Date'] = df['Date_fixed']
    # drop helper cols
    df.drop(columns=['Date_parsed','Date_fixed'], inplace=True)

    df.to_csv(OUT_FILE, index=False)
    print('Saved fixed file to', OUT_FILE)

if __name__ == '__main__':
    main()
