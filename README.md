# Call Report Data Acquisition Pipeline

Scripts for downloading and processing FFIEC 031/041, 002, and FRB 2886b data (1985-2025).

## Overview

Downloads raw data, adds MDRM variable descriptions, separates by filer type, and saves in parquet format with column-level metadata.

## Requirements

```bash
pip install -r requirements.txt
```

## Quick Start

```bash
# 1. Download Chicago Fed data (1985-2021) and the FFIEC CDR bulk files (2011Q1-present)
python 01_download_data.py
python 01b_download_ffiec_cdr.py            # every CDR quarter not yet on disk
python 01b_download_ffiec_cdr.py --check    # newest published vs newest on disk, no download

# 2. Download MDRM data dictionary
python 02_download_dictionary.py

# 3. Parse dictionary for Call Report variables
python 03_parse_dictionary.py

# 4. Parse Chicago Fed data to parquet (with variable descriptions)
python 04_parse_chicago.py

# 5. Parse FFIEC CDR bulk downloads (2011Q1-present)
python 05_parse_ffiec.py

# 6. Summarize parsed data
python 06_summarize.py

# 7. Clean up to free disk space
python 07_cleanup.py --extracted
```

## Pipeline Scripts

| Script | Purpose |
|--------|---------|
| `01_download_data.py` | Download Chicago Fed ZIP files (1985-2021) |
| `01b_download_ffiec_cdr.py` | Download FFIEC CDR "Call Reports -- Single Period" bulk ZIPs (2011Q1-present); `--check` reports newest published vs newest on disk |
| `02_download_dictionary.py` | Download MDRM data dictionary from Federal Reserve |
| `03_parse_dictionary.py` | Parse MDRM for Call Report variable descriptions |
| `04_parse_chicago.py` | Extract Chicago Fed SAS XPORT files to parquet |
| `05_parse_ffiec.py` | Parse FFIEC CDR bulk downloads to parquet |
| `06_summarize.py` | Summarize parsed data by entity type |
| `07_cleanup.py` | Delete raw/processed files to free disk space |

## Data Coverage

| Entity Type | Coverage | Data Source |
|-------------|----------|-------------|
| **FFIEC_031_041** (Commercial Banks) | 1985Q1-2010Q4 | Chicago Fed Historical |
| | 2011Q1-present (2026Q2 as of 2026-09) | FFIEC CDR Bulk Downloads (`01b_download_ffiec_cdr.py`) |
| **FFIEC_002** (Foreign Branches) | 1985Q1-2021Q2 | Chicago Fed |
| **FRB_2886b** (Edge/Agreement Corps) | 1985Q1-2021Q2 | Chicago Fed |

## Data Dictionary Integration

The pipeline adds MDRM variable descriptions as parquet column metadata (similar to Stata variable labels):

```python
import pyarrow.parquet as pq

# Read parquet file
pf = pq.ParquetFile('data/processed/FFIEC_031_041/2020Q1.parquet')

# Get variable description
field = pf.schema_arrow.field('RCON2170')
desc = field.metadata.get(b'description', b'').decode('utf-8')
print(f"RCON2170: {desc}")
# Output: RCON2170: Total assets
```

The dictionary includes descriptions for all MDRM codes: RCON, RCFD, RIAD, RCFA, RCFN, RCFW, RCOA, RCOW.

## Detailed Pipeline Steps

### Chicago Fed download (`01_download_data.py`)

```bash
# Download all available data (1985-2021)
python 01_download_data.py --start-year 1985 --end-year 2021
```

This downloads quarterly ZIP files containing SAS XPORT (.xpt) files.

### FFIEC CDR bulk download, 2011Q1 onward (`01b_download_ffiec_cdr.py`)

For FFIEC_031_041 coverage from 2011Q1 the source is the FFIEC CDR bulk download page,
https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx ("Call Reports -- Single Period",
tab-delimited). The script drives that form (no credentials, no API key), downloads every
quarter the site offers that is not yet in `data/raw/ffiec/`, and saves each ZIP under the
site's own file name. `--check` only reports the newest quarter published against the
newest on disk (exit code 1 when quarters are missing), which is a cheap thing to run
before a rebuild. A manual download to `data/raw/ffiec/` still works if the form changes.

### MDRM data dictionary (`02_download_dictionary.py`, `03_parse_dictionary.py`)

```bash
# Download MDRM.zip from Federal Reserve
python 02_download_dictionary.py

# Parse for Call Report variables
python 03_parse_dictionary.py
```

Creates `data/dictionary/data_dictionary.parquet` with variable descriptions.

### Parse to parquet (`04_parse_chicago.py`, `05_parse_ffiec.py`)

```bash
# Parse Chicago Fed data (auto-extracts ZIPs, separates by entity type)
python 04_parse_chicago.py

# Parse FFIEC bulk downloads
python 05_parse_ffiec.py
```

### Verify and summarize (`06_summarize.py`)

```bash
python 06_summarize.py
```

Shows combined quarterly breakdown by entity type with filer and variable counts.

## Output Format

```
data/processed/
├── FFIEC_031_041/         # Commercial Banks
│   ├── 1985Q1.parquet
│   └── ...
├── FFIEC_002/             # Foreign Bank Branches
│   ├── 1985Q1.parquet
│   └── ...
└── FRB_2886b/             # Edge/Agreement Corporations
    ├── 1985Q1.parquet
    └── ...
```

**Parquet File Structure:**
- **Rows**: One per filer (RSSD_ID)
- **Columns**: `RSSD_ID`, `REPORTING_PERIOD`, MDRM codes (uppercase)
- **Metadata**: Variable descriptions in column metadata

**Typing rules (FFIEC CDR files, 2011Q1 onward):**
- A column is stored as a number only if *every* populated value parses as one; otherwise
  it stays text. Boolean items (`true`/`false`), the capital election code (`RCON6724`),
  the fiscal year-end (`RCON8678`), the LEI (`RCON9224`), names, addresses and `TEXT*`
  labels are therefore text.
- **Confidential items are dropped.** The CDR files write the literal string `CONF` for
  every bank in a confidential item (322 Schedule RC-O assessment items, two RC-P, two
  RC-C and one RI-E; 325–327 columns a quarter from 2013Q4). No item is ever partially
  confidential, so a column that is all `CONF` carries nothing and is not written. The
  parser prints the dropped codes for each file.
- **Percent strings become numbers in percent units.** The reported capital ratios
  (`RCOA`/`RCFA` 7204, 7205, 7206, P793 and the other Basel III ratio lines; 21 columns
  in 2025Q3) are written as `9.1154%` from 2015Q1. A column whose every populated value
  is a percent string is stored as the number written, `9.1154`. Note the same items are
  numeric *fractions* (`0.0948`) in the 2011Q1–2014Q4 CDR files and switch between
  fractions and percent inside the Chicago Fed era (fractions to 2008Q3, percent
  2008Q4–2010Q4). This repository stores what each file says; unit harmonization is the
  consumer's job (see `bankpanel`).
- Text columns are always written as Arrow `string`, so the schema is the same type for
  every quarter.

## Cleanup Utility

```bash
# Delete extracted .xpt files (keeps ZIPs)
python 07_cleanup.py --extracted

# Delete all raw data
python 07_cleanup.py --raw

# Dry run to preview
python 07_cleanup.py --extracted --dry-run
```

## Entity Type Separation

Chicago Fed data contains multiple entity types identified by **RSSD9331**:

| RSSD9331 | Entity Type | Output Directory |
|----------|-------------|------------------|
| 1, 10, 17 | Commercial, savings and co-operative banks (FFIEC 031/041/034) | `FFIEC_031_041/` |
| 9, 11 | U.S. branches and agencies of foreign banks (FFIEC 002) | `FFIEC_002/` |
| 13, 21 | Edge and Agreement corporations (FR 2886b) | `FRB_2886b/` |

The mapping is `ENTITY_TYPES` in `04_parse_chicago.py`. Codes 10 and 17 (domestic savings
banks and co-operative banks) were routed out of `FFIEC_031_041` before 2026-07, which
dropped real banks; they file the Call Report and belong with it.
