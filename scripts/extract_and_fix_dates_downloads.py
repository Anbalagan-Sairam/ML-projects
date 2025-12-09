"""Scan Downloads for health data, normalize Date values at source, merge.

This script improves extraction by normalizing the `Date` column before
concatenation. It handles Excel serials, numeric float serials, YYYYMMDD,
and common textual formats using dateutil.

Outputs:
- data/merged_health_from_downloads_dates_fixed.csv
- data/merged_health_clean_subset_dates_fixed_source.csv (subset)
- data/bad_dates_by_source.csv
"""

import json
from pathlib import Path
import pandas as pd
import re
import traceback
from dateutil.parser import parse

DOWNLOADS = Path.home() / 'Downloads'
REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / 'data'
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_MERGED = OUT_DIR / 'merged_health_from_downloads_dates_fixed.csv'
OUT_CLEAN = OUT_DIR / 'merged_health_clean_subset_dates_fixed_source.csv'
OUT_BAD = OUT_DIR / 'bad_dates_by_source.csv'
FOUND_JSON = OUT_DIR / 'found_health_files_from_downloads.json'

EXPECTED = ['date','weight','nutrition','exercise','sleep','hygiene','food']

excel_epoch = pd.Timestamp('1899-12-30')

def header_has_expected(cols):
    lows = [str(c).lower().strip() for c in cols]
    for e in EXPECTED:
        if e in lows:
            return True
    return False

def try_read_excel(path, sheet_name=None):
    try:
        return pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    except Exception:
        try:
            return pd.read_excel(path, sheet_name=sheet_name, engine='openpyxl', dtype=str)
        except Exception:
            return None

def try_read_csv(path):
    try:
        return pd.read_csv(path, dtype=str, encoding='utf-8', low_memory=False)
    except Exception:
        try:
            return pd.read_csv(path, dtype=str, encoding='latin-1', low_memory=False)
        except Exception:
            return None

def normalize_date_value(v):
    if pd.isna(v):
        return pd.NaT
    s = str(v).strip()
    if s == '' or s.lower() in ['nan','none','na']:
        return pd.NaT
    # strip ordinal suffixes and stray commas
    s = re.sub(r'(?<=\d)(st|nd|rd|th)\b', '', s, flags=re.IGNORECASE).strip().strip(',')
    # normalize floats like 45924.0
    m = re.fullmatch(r"(\d+)\.0+", s)
    if m:
        s = m.group(1)
    # pure numeric -> excel serial or yyyymmdd
    if re.fullmatch(r"\d+", s):
        try:
            n = int(s)
            if n > 29500:
                try:
                    return (excel_epoch + pd.to_timedelta(n, unit='D')).normalize()
                except Exception:
                    pass
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
    # common strptime formats
    for fmt in ['%d/%m/%Y','%d-%m-%Y','%Y-%m-%d','%m/%d/%Y','%d %b %Y','%d %B %Y','%Y.%m.%d']:
        try:
            return pd.to_datetime(s, format=fmt)
        except Exception:
            pass
    # last resort: dateutil
    try:
        return parse(s, dayfirst=False, yearfirst=False)
    except Exception:
        try:
            return parse(s, dayfirst=True, yearfirst=False)
        except Exception:
            return pd.NaT

def find_and_process():
    sources = []
    rows = []
    bad_rows = []
    if not DOWNLOADS.exists():
        print('Downloads path not found:', DOWNLOADS)
        return sources, rows, bad_rows

    for p in DOWNLOADS.rglob('*'):
        if not p.is_file():
            continue
        lowname = p.name.lower()
        try:
            if p.suffix.lower() == '.csv':
                include = False
                reason = None
                if 'health' in lowname:
                    include = True
                    reason = 'filename contains health'
                else:
                    # quick header check
                    try:
                        head = pd.read_csv(p, nrows=0, dtype=str)
                        if header_has_expected(head.columns):
                            include = True
                            reason = 'header matched expected columns'
                    except Exception:
                        include = False
                if include:
                    df = try_read_csv(p)
                    if df is None:
                        continue
                    df['source_file'] = str(p)
                    df['source_sheet'] = ''
                    # normalize date column if present
                    date_col = None
                    for c in df.columns:
                        if 'date' == str(c).lower().strip() or 'date' in str(c).lower():
                            date_col = c
                            break
                    if date_col is not None:
                        df['Date_normalized'] = df[date_col].apply(normalize_date_value)
                    else:
                        df['Date_normalized'] = pd.NaT
                    # record rows
                    for _, r in df.iterrows():
                        rows.append(r.to_dict())
                        if pd.isna(r['Date_normalized']):
                            bad_rows.append({'source_file': str(p), 'source_sheet': '', 'date_raw': r.get(date_col, '')})
                    sources.append({'path': str(p), 'reason': reason, 'sheet': ''})

            elif p.suffix.lower() in ['.xls', '.xlsx', '.xlsm', '.xlsb']:
                try:
                    xls = pd.ExcelFile(p)
                except Exception:
                    continue
                # prefer sheet names with 'health'
                target_sheets = []
                for s in xls.sheet_names:
                    if 'health' in s.lower():
                        target_sheets.append(s)
                # else check headers
                if not target_sheets:
                    for s in xls.sheet_names:
                        try:
                            head = pd.read_excel(p, sheet_name=s, nrows=0)
                            if header_has_expected(head.columns):
                                target_sheets.append(s)
                                break
                        except Exception:
                            continue
                # if still empty, skip
                for s in target_sheets:
                    df = try_read_excel(p, sheet_name=s)
                    if df is None:
                        continue
                    df['source_file'] = str(p)
                    df['source_sheet'] = s
                    date_col = None
                    for c in df.columns:
                        if 'date' == str(c).lower().strip() or 'date' in str(c).lower():
                            date_col = c
                            break
                    if date_col is not None:
                        df['Date_normalized'] = df[date_col].apply(normalize_date_value)
                    else:
                        df['Date_normalized'] = pd.NaT
                    for _, r in df.iterrows():
                        rows.append(r.to_dict())
                        if pd.isna(r['Date_normalized']):
                            bad_rows.append({'source_file': str(p), 'source_sheet': s, 'date_raw': r.get(date_col, '')})
                    sources.append({'path': str(p), 'reason': 'sheet matched', 'sheet': s})
        except Exception:
            print('Error processing', p)
            traceback.print_exc()

    return sources, rows, bad_rows

def save_outputs(sources, rows, bad_rows):
    # write sources
    with open(FOUND_JSON, 'w', encoding='utf-8') as f:
        json.dump(sources, f, indent=2)

    if not rows:
        print('No rows collected')
        return
    df_all = pd.DataFrame(rows)
    # coerce Date_normalized to datetime then iso
    if 'Date_normalized' in df_all.columns:
        df_all['Date_normalized'] = pd.to_datetime(df_all['Date_normalized'], errors='coerce')
        df_all['Date_normalized'] = df_all['Date_normalized'].dt.strftime('%Y-%m-%d')
    df_all.to_csv(OUT_MERGED, index=False)
    print('Wrote merged (with source-normalized dates):', OUT_MERGED, 'shape=', df_all.shape)

    # cleaned subset
    col_map = {str(c).lower().strip(): c for c in df_all.columns}
    subset_cols = []
    for want in ['Date','Weight','Nutrition','Exercise','Sleep','Hygiene','Food']:
        key = want.lower()
        if key in col_map:
            subset_cols.append(col_map[key])
        else:
            found = None
            for low, orig in col_map.items():
                if key in low and orig not in subset_cols:
                    found = orig
                    break
            subset_cols.append(found)

    clean = pd.DataFrame()
    # prefer using Date_normalized if present
    if 'Date_normalized' in df_all.columns:
        clean['Date'] = df_all['Date_normalized']
    else:
        # fallback to any Date-like column
        date_col = subset_cols[0]
        clean['Date'] = df_all[date_col] if date_col else None

    for i, want in enumerate(['Weight','Nutrition','Exercise','Sleep','Hygiene','Food']):
        src = subset_cols[i+1]
        clean[want] = df_all[src] if src else None

    for extra in ['source_file','source_sheet']:
        if extra in df_all.columns:
            clean[extra] = df_all[extra]

    clean.to_csv(OUT_CLEAN, index=False)
    print('Wrote cleaned subset:', OUT_CLEAN, 'shape=', clean.shape)

    # bad rows
    if bad_rows:
        pd.DataFrame(bad_rows).to_csv(OUT_BAD, index=False)
        print('Wrote bad date rows to', OUT_BAD, 'count=', len(bad_rows))

if __name__ == '__main__':
    print('Scanning and normalizing dates from', DOWNLOADS)
    sources, rows, bad_rows = find_and_process()
    print('Found', len(sources), 'sources; rows collected=', len(rows))
    save_outputs(sources, rows, bad_rows)
    print('Done')
