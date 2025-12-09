import pandas as pd
import re
from pathlib import Path
from dateutil.parser import parse

IN = Path(r"C:/Users/anbal/Documents/GitHub/ML-projects/data/merged_health.csv")
OUT = Path(r"C:/Users/anbal/Documents/GitHub/ML-projects/data/merged_health_clean_v2.csv")
BAD = Path(r"C:/Users/anbal/Documents/GitHub/ML-projects/data/bad_date_rows.csv")

print('Loading', IN)
df = pd.read_csv(IN, dtype=str, encoding='utf-8', low_memory=False)
keep_cols = ['Date','Weight','Nutrition','Exercise','Sleep','Hygiene','Food']
# map present columns case-insensitive
cols_lower = {c.lower(): c for c in df.columns}
present = []
for k in keep_cols:
    if k.lower() in cols_lower:
        present.append(cols_lower[k.lower()])
    else:
        for low, orig in cols_lower.items():
            if k.lower() in low and orig not in present:
                present.append(orig)
                break

if not present:
    raise SystemExit('No target columns found')

print('Keeping columns:', present)
df_sub = df.loc[:, present].copy()
# keep original raw date
raw_col = present[0] if present[0].lower().startswith('date') else None
if raw_col is None:
    # try find any date-like column
    for c in df_sub.columns:
        if 'date' in c.lower():
            raw_col = c
            break

if raw_col is None:
    raise SystemExit('No Date column found')

# rename to canonical names
rename_map = {raw_col: 'Date'}
for c in df_sub.columns:
    if c != raw_col:
        rename_map[c] = c.strip().capitalize()

df_sub.rename(columns=rename_map, inplace=True)
df_sub['Date_raw'] = df_sub['Date'].astype(str)

# helper functions
excel_epoch = pd.Timestamp('1899-12-30')

def try_parse_any(s):
    if pd.isna(s):
        return pd.NaT
    s0 = str(s).strip()
    if s0 == '' or s0.lower() in ['nan','none','na']:
        return pd.NaT
    # remove ordinal suffixes (1st, 2nd, 3rd, 4th)
    s1 = re.sub(r'(?<=\d)(st|nd|rd|th)\b', '', s0, flags=re.IGNORECASE)
    # normalize floats like 45909.0 -> 45909
    m_float = re.fullmatch(r"(\d+)\.0+", s1)
    if m_float:
        s1 = m_float.group(1)
    # also trim trailing commas or stray characters
    s1 = s1.strip().strip(',')
    # If purely numeric
    try:
        if re.fullmatch(r"\d+", s1):
            num = int(s1)
            # Excel serial number? typical range > 20000
            if num > 29500:
                try:
                    dt = excel_epoch + pd.to_timedelta(num, unit='D')
                    return dt
                except Exception:
                    pass
            # maybe YYYYMMDD
            if len(s1) == 8:
                try:
                    return pd.to_datetime(s1, format='%Y%m%d')
                except Exception:
                    pass
            if len(s1) == 6:
                try:
                    return pd.to_datetime(s1, format='%y%m%d')
                except Exception:
                    pass
    except Exception:
        pass
    # common separators and time-only
    for fmt in ['%d/%m/%Y','%d-%m-%Y','%Y-%m-%d','%m/%d/%Y','%d %b %Y','%d %B %Y','%Y.%m.%d','%d.%m.%Y','%H:%M:%S']:
        try:
            return pd.to_datetime(s1, format=fmt)
        except Exception:
            pass
    # try dateutil parse (try both orders)
    try:
        return parse(s1, dayfirst=False, yearfirst=False)
    except Exception:
        try:
            return parse(s1, dayfirst=True, yearfirst=False)
        except Exception:
            pass
    return pd.NaT

print('Attempting robust date parsing...')
df_sub['Date_parsed'] = df_sub['Date_raw'].apply(try_parse_any)
# Count failures
fail_mask = df_sub['Date_parsed'].isna()
num_fail = int(fail_mask.sum())
print('Parsed dates; failures:', num_fail)

# Try additional heuristics on failures: extract numeric groups or Excel-like floats
if num_fail > 0:
    def second_try(x):
        s = str(x)
        # if contains a number like 45909.0 or large digits
        m = re.search(r"(\d+\.0+|\d{5,8})", s)
        if m:
            token = m.group(1)
            token = token.split('.')[0]
            return try_parse_any(token)
        # extract first group that looks like a date with separators
        m2 = re.search(r"(\d{1,4}[\-/\.\s]\d{1,2}[\-/\.\s]\d{1,4})", s)
        if m2:
            return try_parse_any(m2.group(1))
        return pd.NaT
    df_sub.loc[fail_mask, 'Date_parsed'] = df_sub.loc[fail_mask, 'Date_raw'].apply(second_try)
    num_fail2 = int(df_sub['Date_parsed'].isna().sum())
    print('After second pass failures:', num_fail2)

# Format dates to ISO, keep NaT as empty
df_sub['Date'] = pd.to_datetime(df_sub['Date_parsed'], errors='coerce')
# Keep date only
df_sub['Date'] = df_sub['Date'].dt.strftime('%Y-%m-%d')

# Save bad rows for review
bad = df_sub[df_sub['Date'].isna()].copy()
if not bad.empty:
    print('Saving', len(bad), 'bad date rows to', BAD)
    bad.to_csv(BAD, index=False)
else:
    print('No bad date rows found')

# Build final columns
final_cols = ['Date','Date_raw'] + [c for c in ['Weight','Nutrition','Exercise','Sleep','Hygiene','Food'] if c in df_sub.columns]
final = df_sub[final_cols]
print('Final shape:', final.shape)
final.to_csv(OUT, index=False)
print('Saved cleaned file to', OUT)
import pandas as pd
import re
from pathlib import Path
from dateutil.parser import parse

IN = Path(r"C:/Users/anbal/Documents/GitHub/ML-projects/data/merged_health.csv")
OUT = Path(r"C:/Users/anbal/Documents/GitHub/ML-projects/data/merged_health_clean_v2.csv")
BAD = Path(r"C:/Users/anbal/Documents/GitHub/ML-projects/data/bad_date_rows.csv")

print('Loading', IN)
df = pd.read_csv(IN, dtype=str, encoding='utf-8', low_memory=False)
keep_cols = ['Date','Weight','Nutrition','Exercise','Sleep','Hygiene','Food']
# map present columns case-insensitive
cols_lower = {c.lower(): c for c in df.columns}
present = []
for k in keep_cols:
    if k.lower() in cols_lower:
        present.append(cols_lower[k.lower()])
    else:
        for low, orig in cols_lower.items():
            if k.lower() in low and orig not in present:
                present.append(orig)
                break

if not present:
    raise SystemExit('No target columns found')

print('Keeping columns:', present)
df_sub = df.loc[:, present].copy()
# keep original raw date
raw_col = present[0] if present[0].lower().startswith('date') else None
if raw_col is None:
    # try find any date-like column
    for c in df_sub.columns:
        if 'date' in c.lower():
            raw_col = c
            break

if raw_col is None:
    raise SystemExit('No Date column found')

# rename to canonical names
rename_map = {raw_col: 'Date'}
for c in df_sub.columns:
    if c != raw_col:
        rename_map[c] = c.strip().capitalize()

df_sub.rename(columns=rename_map, inplace=True)
df_sub['Date_raw'] = df_sub['Date'].astype(str)

# helper functions
excel_epoch = pd.Timestamp('1899-12-30')

def try_parse_any(s):
    if pd.isna(s):
        return pd.NaT
    s0 = str(s).strip()
    if s0 == '' or s0.lower() in ['nan','none','na']:
        return pd.NaT
    # remove ordinal suffixes (1st, 2nd, 3rd, 4th)
    s1 = re.sub(r'(?<=\d)(st|nd|rd|th)\b', '', s0, flags=re.IGNORECASE)
    # If purely numeric
    digits = re.sub(r'[^0-9]','', s1)
    try:
        # Excel serial number? typical range > 20000
        if re.fullmatch(r"\d+", s1):
            num = int(s1)
            if num > 29500:  # roughly dates after 1980
                try:
                    dt = excel_epoch + pd.to_timedelta(num, unit='D')
                    return dt
                except Exception:
                    pass
            # maybe YYYYMMDD
            if len(s1) == 8:
                try:
                    return pd.to_datetime(s1, format='%Y%m%d')
                except Exception:
                    pass
            if len(s1) == 6:
                # try YYMMDD
                try:
                    return pd.to_datetime(s1, format='%y%m%d')
                except Exception:
                    pass
        # common separators
        for fmt in ['%d/%m/%Y','%d-%m-%Y','%Y-%m-%d','%m/%d/%Y','%d %b %Y','%d %B %Y','%Y.%m.%d','%d.%m.%Y']:
            try:
                return pd.to_datetime(s1, format=fmt)
            except Exception:
                pass
        # try dateutil parse (try both orders)
        try:
            return parse(s1, dayfirst=False, yearfirst=False)
        except Exception:
            try:
                return parse(s1, dayfirst=True, yearfirst=False)
            except Exception:
                pass
    except Exception:
        pass
    return pd.NaT

print('Attempting robust date parsing...')
df_sub['Date_parsed'] = df_sub['Date_raw'].apply(try_parse_any)
# Count failures
fail_mask = df_sub['Date_parsed'].isna()
num_fail = fail_mask.sum()
print('Parsed dates; failures:', num_fail)

# Try additional heuristics on failures: remove text, keep first number group
if num_fail > 0:
    def second_try(x):
        s = str(x)
        # extract first group that looks like a date (contains digits and separators)
        m = re.search(r"(\d{1,4}[\-/\.\s]\d{1,2}[\-/\.\s]\d{1,4})", s)
        if m:
            return try_parse_any(m.group(1))
        # if year at end like 'Apr 27 2025' handled earlier
        return pd.NaT
    df_sub.loc[fail_mask, 'Date_parsed'] = df_sub.loc[fail_mask, 'Date_raw'].apply(second_try)
    num_fail2 = df_sub['Date_parsed'].isna().sum()
    print('After second pass failures:', num_fail2)

# Format dates to ISO, keep NaT as empty
df_sub['Date'] = pd.to_datetime(df_sub['Date_parsed'], errors='coerce')
# Keep date only
df_sub['Date'] = df_sub['Date'].dt.strftime('%Y-%m-%d')

# Save bad rows for review
bad = df_sub[df_sub['Date'].isna()].copy()
if not bad.empty:
    print('Saving', len(bad), 'bad date rows to', BAD)
    bad.to_csv(BAD, index=False)
else:
    print('No bad date rows found')

# Drop helper cols and reorder
cols_keep = ['Date','Date_raw'] + [c for c in df_sub.columns if c not in ['Date','Date_raw','Date_parsed']]
# but ensure we only keep requested columns and Date_raw
final_cols = ['Date','Date_raw'] + [c for c in ['Weight','Nutrition','Exercise','Sleep','Hygiene','Food'] if c in df_sub.columns]
final = df_sub[final_cols]
print('Final shape:', final.shape)
final.to_csv(OUT, index=False)
print('Saved cleaned file to', OUT)
