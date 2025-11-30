# Call Report Data Acquisition Pipeline

This folder contains scripts for downloading and extracting FFIEC Call Report data (1985-2025).

## Overview

This downloads raw data and converts it to parquet format without any project-specific transformations.

## Requirements

Install required Python packages before running the scripts:

```bash
pip install -r requirements.txt
```

## Two-Step Pipeline

### Step 1: Download Data

Data is obtained from two sources for complete coverage (1985-2025):

#### A. Chicago Fed Historical Data (1985-2000) - **AUTOMATED**

64 quarters of historical data downloaded automatically from Chicago Fed:

```bash
python download.py --start-year 1985 --end-year 2000 --output-dir data/raw/chicago
```

**Note:** Chicago Fed data is available through 2010 (download script supports up to 2010). Some workflows limit Chicago data to 1985-2000 for consistency, while FFIEC data is used for 2001+ when needed.

#### B. FFIEC Bulk Downloads (2001-present) - **MANUAL**

Requires manual download from FFIEC:

1. Visit: https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx
2. For each quarter:
   - Select "Call Reports -- Single Period"
   - Choose quarter end date (e.g., 06/30/2024)
   - Select "Tab Delimited" format
   - Click Download
3. Save files to: `data/raw/ffiec/`

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
python parse_chicago.py \
  --input-dir data/raw/chicago \
  --output-dir data/processed
```

#### B. Parse FFIEC Bulk Files

```bash
python parse_ffiec.py \
  --input-dir data/raw/ffiec \
  --output-dir data/processed
```

## Output Format

All data is saved as parquet files:

```
data/processed/
├── 1985Q1.parquet
├── 1985Q2.parquet
├── ...
├── 2025Q3.parquet
```

**File Structure:**
- **Rows**: One per bank (RSSD_ID)
- **Columns**:
  - `RSSD_ID`: Bank identifier (integer)
  - `REPORTING_PERIOD`: Quarter end date (datetime)
  - MDRM codes (e.g., `RCON2170`, `RIAD4340`) - all uppercase
  - All numeric values preserved as-is

**Coverage**:
- 163 total quarters (1985Q1 - 2025Q3)
- ~7,000-10,000 banks per quarter
- ~1,000+ MDRM codes per quarter
- Complete data with no filtering or transformations

## Pipeline Scripts

### Core Scripts (Active)

| Script | Purpose | Input | Output | Performance |
|--------|---------|-------|--------|-------------|
| `download.py` | Download Chicago Fed historical data (1985-2000) | URLs | ZIP files | ~2 min |
| `parse_chicago.py` | Extract SAS XPORT files to parquet (parallel) | .xpt files | Parquet files | ~3-5 min (64 files) |
| `parse_ffiec.py` | Parse FFIEC bulk downloads to parquet (parallel) | .txt/.zip files | Parquet files | ~2-4 min (59 files) |
| `summarize.py` | Summarize parsed data (quarters, banks, variables) | Parquet files | Summary table | ~10-30 sec (163 files) |

**Parallelization Options** (available for `parse_chicago.py`, `parse_ffiec.py`, `summarize.py`):
- **Default**: Uses all CPU cores for parallel processing
- `--workers N`: Specify number of parallel workers (e.g., `--workers 4`)
- `--no-parallel`: Disable parallel processing (slower but uses less memory)
- **Expected speedup**: 4-8x on multi-core CPUs compared to sequential processing

**Examples**:
```bash
# Use default parallel processing (all CPU cores)
python summarize.py --input-dir data/processed

# Limit to 4 workers
python parse_chicago.py --input-dir data/raw/chicago --output-dir data/processed --workers 4

# Disable parallelization (for low-memory systems)
python parse_ffiec.py --input-dir data/raw/ffiec --output-dir data/processed --no-parallel
```
