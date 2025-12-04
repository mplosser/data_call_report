# Call Report Data Acquisition Pipeline

This folder contains scripts for downloading and extracting FFIEC 0031/041. 002 and FRB2886b data (1985-2025).

## Overview

Downloads raw data, separates it by filer types and saves in parquet format.

## Requirements

Install required Python packages before running the scripts:

```bash
pip install -r requirements.txt
```

## Two-Step Pipeline

### Complete Coverage Summary

This pipeline provides complete coverage for all entity types using three data sources:

| Entity Type | Coverage Period | Data Source |
|-------------|----------------|-------------|
| **FFIEC_031_041** (Commercial Banks) | 1985Q1-2010Q4 | Chicago Fed Historical |
| | 2011Q1-2025Q3 | FFIEC CDR Bulk Downloads |
| **FFIEC_002** (Foreign Branches) | 1985Q1-2010Q4 | Chicago Fed Historical |
| | 2011Q1-2021Q2 | Chicago Fed Structure Data |
| **FRB_2886b** (Edge/Agreement) | 1985Q1-2010Q4 | Chicago Fed Historical |
| | 2011Q1-2021Q2 | Chicago Fed Structure Data |

**Total Coverage:**
- FFIEC_031_041: 163 quarters (1985Q1-2025Q3)
- FFIEC_002: 146 quarters (1985Q1-2021Q2) - ended
- FRB_2886b: 146 quarters (1985Q1-2021Q2) - ended

### Step 1: Download Data

Data is obtained from three sources for complete coverage (1985-2025):

#### A. Chicago Fed Data (1985-2021) - **AUTOMATED**

Two datasets from Chicago Fed cover different periods:

**1. Historical Call Reports (1985-2010)** - All entity types:
```bash
python download.py --start-year 1985 --end-year 2010 --output-dir data/raw/chicago
```

**2. Structure Data (2011-2021Q2)** - FFIEC_002 & FRB_2886b only:
```bash
python download.py --start-year 2011 --end-year 2021 --output-dir data/raw/chicago
```

**Download both for complete Chicago Fed coverage:**
```bash
python download.py --start-year 1985 --end-year 2021 --output-dir data/raw/chicago
```

**IMPORTANT:**
- Structure data (2011-2021) contains FFIEC_002 and FRB_2886b only
- **parse_chicago.py automatically excludes FFIEC_031_041 for quarters after 2010Q4**
- Chicago Fed structure data has FFIEC_031_041 but with only ~194 variables (vs ~1,000 in FFIEC CDR)
- For FFIEC_031_041 (Commercial Banks) 2011+, you MUST use FFIEC CDR below

#### B. FFIEC Bulk Downloads (2011-present) - **MANUAL**

Required for FFIEC_031_041 (Commercial Banks) coverage 2011+. Manual download from FFIEC:

1. Visit: https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx
2. For each quarter (2011Q1 onwards):
   - Select "Call Reports -- Single Period"
   - Choose quarter end date (e.g., 06/30/2024)
   - Select "Tab Delimited" format
   - Click Download
3. Save files to: `data/raw/ffiec/`

**Note:** FFIEC CDR only contains Commercial Banks (FFIEC 031/041). For FFIEC_002 and FRB_2886b (2011-2021), use Chicago Fed Structure Data above.

### Step 2: Extract to Parquet

Convert downloaded files to standardized parquet format:

#### Default input/output locations and behaviour

- parse_chicago.py (default):
  - Input: `data/raw/chicago` — the script will search both this directory and
    an `extracted/` subdirectory for `.zip` and `.xpt` files. If ZIPs are found
    in `data/raw/chicago` they will be extracted into `data/raw/chicago/extracted`.
  - Output: `data/processed`

- parse_ffiec.py (default):
  - Input: `data/raw/ffiec` — expects bulk FFIEC files (text or zip).
  - Output: `data/processed`

These defaults let you run the parsers without any flags when you place files
into the repository layout shown above. You can still override with
`--input-dir` and `--output-dir` when you need custom locations.

#### A. Extract and Parse Chicago Fed Data

```bash
python parse_chicago.py
```

The script automatically separates filers by entity type into subdirectories:
- `FFIEC_031_041/` - Commercial Banks (FFIEC 031/041)
- `FFIEC_002/` - Foreign Bank Branches (FFIEC 002)
- `FRB_2886b/` - Edge/Agreement Corporations (FR 2886b)

**Disk Space Management:**
- Automatically extracts ZIP files to `data/raw/chicago/extracted/`
- Original ZIP files are preserved
- Use `cleanup.py` to delete extracted files and free disk space (see Cleanup section below)

**Entity Types:**

Chicago Fed data contains multiple entity types identified by the **RSSD9331** field:

| RSSD9331 | Entity Type | Report Form | Output Directory | % (2000Q1) |
|----------|-------------|-------------|------------------|-----------|
| 1 | Commercial Bank | FFIEC 031/041 | `FFIEC_031_041/` | 89.7% |
| 10, 11 | Foreign Bank Branch/Agency | FFIEC 002 | `FFIEC_002/` | 5.7% |
| 13, 17 | Edge/Agreement Corporation | FR 2886b | `FRB_2886b/` | 1.3% |
| 9, 21, 22, 24 | Other (bankers banks, trusts, etc.) | Various | *Not included* | 3.3% |

**Date Filtering:**

```bash
# Process specific date range
python parse_chicago.py --start-date 2000-01-01 --end-date 2000-12-31
```

#### B. Parse FFIEC Bulk Files

```bash
python parse_ffiec.py
```

FFIEC CDR bulk downloads contain only FFIEC 031/041 commercial banks, so they're automatically saved to the `FFIEC_031_041/` subdirectory.

## Output Format

All data is organized by entity type in subdirectories:

```
data/processed/
├── FFIEC_031_041/         # Commercial Banks (FFIEC 031/041)
│   ├── 1985Q1.parquet     # Chicago Fed Historical (1985-2010)
│   ├── ...
│   ├── 2010Q4.parquet
│   ├── 2011Q1.parquet     # FFIEC CDR (2011-2025)
│   ├── ...
│   └── 2025Q3.parquet
├── FFIEC_002/             # Foreign Bank Branches (FFIEC 002)
│   ├── 1985Q1.parquet     # Chicago Fed Historical (1985-2010)
│   ├── ...
│   ├── 2010Q4.parquet
│   ├── 2011Q1.parquet     # Chicago Fed Structure (2011-2021)
│   ├── ...
│   └── 2021Q2.parquet     # Last available quarter
└── FRB_2886b/             # Edge/Agreement Corporations (FR 2886b)
    ├── 1985Q1.parquet     # Chicago Fed Historical (1985-2010)
    ├── ...
    ├── 2010Q4.parquet
    ├── 2011Q1.parquet     # Chicago Fed Structure (2011-2021)
    ├── ...
    └── 2021Q2.parquet     # Last available quarter
```

**File Structure:**
- **Rows**: One per filer (RSSD_ID)
- **Columns**:
  - `RSSD_ID`: Filer identifier (integer)
  - `REPORTING_PERIOD`: Quarter end date (datetime)
  - MDRM codes (e.g., `RCON2170`, `RIAD4340`) - all uppercase
  - All numeric values preserved as-is

**Coverage**:
- **FFIEC_031_041** (Commercial Banks): 163 quarters (1985Q1-2025Q3)
  - Chicago Fed Historical: 1985Q1-2010Q4 (104 quarters), ~8,500 banks/quarter, ~2,400 variables
  - FFIEC CDR: 2011Q1-2025Q3 (59 quarters), ~4,500-5,000 banks/quarter, ~4,000 variables

- **FFIEC_002** (Foreign Branches): 146 quarters (1985Q1-2021Q2)
  - Chicago Fed Historical: 1985Q1-2010Q4 (104 quarters), ~400-500 filers/quarter, ~2,400 variables
  - Chicago Fed Structure: 2011Q1-2021Q2 (42 quarters), ~350-400 filers/quarter, ~1,400 variables
  - Note: No data available after 2021Q2

- **FRB_2886b** (Edge/Agreement): 146 quarters (1985Q1-2021Q2)
  - Chicago Fed Historical: 1985Q1-2010Q4 (104 quarters), ~100-250 filers/quarter, ~2,400 variables
  - Chicago Fed Structure: 2011Q1-2021Q2 (42 quarters), ~50-70 filers/quarter, ~1,400 variables
  - Note: No data available after 2021Q2

## Pipeline Scripts

### Core Scripts (Active)

| Script | Purpose | Input | Output |
|--------|---------|-------|--------|
| `download.py` | Download Chicago Fed data (1985-2021) | URLs | ZIP files |
| `parse_chicago.py` | Extract SAS XPORT files to parquet | .xpt/.zip files | Parquet files |
| `parse_ffiec.py` | Parse FFIEC bulk downloads to parquet | .txt/.zip files | Parquet files |
| `summarize.py` | Summarize parsed data by entity type | Parquet files | Summary tables |
| `cleanup.py` | Delete raw/processed files to free disk space | Data directories | Deleted files |

**Parallelization Options** (available for `parse_ffiec.py`, `summarize.py`):
- **Default**: Uses all CPU cores for parallel processing
- `--workers N`: Specify number of parallel workers (e.g., `--workers 4`)
- `--no-parallel`: Disable parallel processing (slower but uses less memory)

**Examples**:
```bash
# Use default parallel processing (all CPU cores)
python summarize.py --input-dir data/processed

# Limit to 4 workers
python parse_ffiec.py --input-dir data/raw/ffiec --output-dir data/processed --workers 4

# Disable parallelization (for low-memory systems)
python summarize.py --input-dir data/processed --no-parallel
```

## Cleanup Utility

The `cleanup.py` script helps manage disk space by deleting downloaded and extracted files.

### Cleanup Options

```bash
# Delete only extracted .xpt files (keeps original ZIPs)
python cleanup.py --extracted

# Delete all raw data (ZIPs + extracted files)
python cleanup.py --raw

# Delete processed parquet files
python cleanup.py --processed

# Delete everything (raw + processed)
python cleanup.py --all

# Dry run to see what would be deleted (without actually deleting)
python cleanup.py --extracted --dry-run
```

### Typical Workflow

```bash
# 1. Download data
python download.py --start-year 1985 --end-year 2021

# 2. Parse to parquet
python parse_chicago.py

# 3. Free up space by deleting extracted files (keeps ZIPs)
python cleanup.py --extracted

# Or delete all raw data if you no longer need it
python cleanup.py --raw
```

**Disk Space Savings:**
- `--extracted`: Frees ~50-70% of extracted directory space
- `--raw`: Frees 100% of raw data directory space
- Always use `--dry-run` first to see what will be deleted
