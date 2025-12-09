"""Scan Downloads recursively for health data files/sheets and merge them.

Behavior:
- Look for files with 'health' in the filename (case-insensitive), and include them.
- For other CSV/Excel files, inspect headers (or sheet names) and include sheets/files
  that contain any of the expected health columns.
- Expected columns (case-insensitive): Date, Weight, Nutrition, Exercise, Sleep, Hygiene, Food
- Saves:
  - data/found_health_files_from_downloads.json  (list of sources)
  - data/merged_health_from_downloads.csv
  - data/merged_health_clean_subset.csv (subset with canonical columns)

Run: python .\scripts\extract_health_from_downloads.py
"""

import json
from pathlib import Path
import pandas as pd
import traceback

DOWNLOADS = Path.home() / 'Downloads'
REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / 'data'
OUT_DIR.mkdir(parents=True, exist_ok=True)

FOUND_JSON = OUT_DIR / 'found_health_files_from_downloads.json'
MERGED_CSV = OUT_DIR / 'merged_health_from_downloads.csv'
MERGED_CLEAN = OUT_DIR / 'merged_health_clean_subset.csv'

EXPECTED = ['date','weight','nutrition','exercise','sleep','hygiene','food']

def header_has_expected(cols):
    lows = [str(c).lower().strip() for c in cols]
    for e in EXPECTED:
        if e in lows:
            return True
    # also accept partial matches like 'wt' or 'sleep_hours' could be noisy, so keep simple
    return False

def try_read_excel(path, sheet_name=None):
    try:
        return pd.read_excel(path, sheet_name=sheet_name, dtype=str)
    except Exception:
        try:
            # try engine fallback
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

def find_and_load():
    sources = []
    frames = []
    if not DOWNLOADS.exists():
        print('Downloads folder not found at', DOWNLOADS)
        return sources, frames

    for p in DOWNLOADS.rglob('*'):
        if p.is_file():
            lowname = p.name.lower()
            try:
                if p.suffix.lower() in ['.csv']:
                    if 'health' in lowname:
                        df = try_read_csv(p)
                        if df is not None:
                            df['source_file'] = str(p)
                            df['source_sheet'] = ''
                            frames.append(df)
                            sources.append({'path': str(p), 'reason': 'filename contains health', 'sheet': ''})
                            print('Included (filename match):', p)
                        continue
                    # else inspect header
                    head = None
                    try:
                        head = pd.read_csv(p, nrows=0, dtype=str)
                    except Exception:
                        pass
                    if head is not None and header_has_expected(head.columns):
                        df = try_read_csv(p)
                        if df is not None:
                            df['source_file'] = str(p)
                            df['source_sheet'] = ''
                            frames.append(df)
                            sources.append({'path': str(p), 'reason': 'header matched expected columns', 'sheet': ''})
                            print('Included (header match):', p)

                elif p.suffix.lower() in ['.xls', '.xlsx', '.xlsm', '.xlsb']:
                    # if filename contains health, try to read all sheets or prefer sheet named health
                    try:
                        xls = pd.ExcelFile(p)
                    except Exception:
                        # skip unreadable
                        continue

                    # sheet name match
                    matched_sheet = None
                    for s in xls.sheet_names:
                        if 'health' == s.lower().strip() or 'health' in s.lower():
                            matched_sheet = s
                            break

                    if matched_sheet is not None:
                        df = try_read_excel(p, sheet_name=matched_sheet)
                        if df is not None:
                            df['source_file'] = str(p)
                            df['source_sheet'] = matched_sheet
                            frames.append(df)
                            sources.append({'path': str(p), 'reason': 'sheet name contains health', 'sheet': matched_sheet})
                            print('Included (sheet name match):', p, '->', matched_sheet)
                        continue

                    # else inspect each sheet header for expected columns
                    for s in xls.sheet_names:
                        try:
                            head = pd.read_excel(p, sheet_name=s, nrows=0)
                        except Exception:
                            head = None
                        if head is not None and header_has_expected(head.columns):
                            df = try_read_excel(p, sheet_name=s)
                            if df is not None:
                                df['source_file'] = str(p)
                                df['source_sheet'] = s
                                frames.append(df)
                                sources.append({'path': str(p), 'reason': 'sheet header matched expected columns', 'sheet': s})
                                print('Included (sheet header match):', p, '->', s)
                                break
            except Exception:
                print('Error processing', p)
                traceback.print_exc()

    return sources, frames

def normalize_and_save(sources, frames):
    # save sources metadata
    with open(FOUND_JSON, 'w', encoding='utf-8') as f:
        json.dump(sources, f, indent=2)

    if not frames:
        print('No health-like data found')
        return

    # standard concat with union of columns
    combined = pd.concat(frames, axis=0, ignore_index=True, sort=False)
    # drop completely empty rows
    combined.dropna(how='all', inplace=True)
    # write merged raw
    combined.to_csv(MERGED_CSV, index=False)
    print('Wrote merged file:', MERGED_CSV, 'shape=', combined.shape)

    # produce cleaned subset with canonical column names (case-insensitive mapping)
    col_map = {str(c).lower().strip(): c for c in combined.columns}
    subset_cols = []
    for want in ['Date','Weight','Nutrition','Exercise','Sleep','Hygiene','Food']:
        key = want.lower()
        if key in col_map:
            subset_cols.append(col_map[key])
        else:
            # try find columns that contain the token
            found = None
            for low, orig in col_map.items():
                if key in low and orig not in subset_cols:
                    found = orig
                    break
            if found:
                subset_cols.append(found)
            else:
                # keep missing columns as empty later
                subset_cols.append(None)

    # build cleaned df
    clean = pd.DataFrame()
    for i, want in enumerate(['Date','Weight','Nutrition','Exercise','Sleep','Hygiene','Food']):
        src = subset_cols[i]
        if src is None:
            clean[want] = None
        else:
            clean[want] = combined[src]

    # keep source columns for debugging
    if 'source_file' in combined.columns:
        clean['source_file'] = combined['source_file']
    if 'source_sheet' in combined.columns:
        clean['source_sheet'] = combined['source_sheet']

    clean.to_csv(MERGED_CLEAN, index=False)
    print('Wrote cleaned subset:', MERGED_CLEAN, 'shape=', clean.shape)


if __name__ == '__main__':
    print('Scanning', DOWNLOADS)
    sources, frames = find_and_load()
    print('Found', len(sources), 'candidate sheets/files')
    normalize_and_save(sources, frames)
    print('Done')
