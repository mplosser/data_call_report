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
# 1. Download Chicago Fed data (1985-2021)
python 01_download_data.py

# 2. Download MDRM data dictionary
python 02_download_dictionary.py

# 3. Parse dictionary for Call Report variables
python 03_parse_dictionary.py

# 4. Parse Chicago Fed data to parquet (with variable descriptions)
python 04_parse_chicago.py

# 5. Parse FFIEC bulk downloads (2011+, manual download required)
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
| | 2011Q1-2025Q3 | FFIEC CDR Bulk Downloads |
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

### Step 1: Download Chicago Fed Data

```bash
# Download all available data (1985-2021)
python 01_download_data.py --start-year 1985 --end-year 2021
```

This downloads quarterly ZIP files containing SAS XPORT (.xpt) files.

### Step 2: Download FFIEC Bulk Data (Manual)

For FFIEC_031_041 coverage 2011+, manually download from FFIEC:

1. Visit: https://cdr.ffiec.gov/public/PWS/DownloadBulkData.aspx
2. Select "Call Reports -- Single Period"
3. Choose quarter end date and "Tab Delimited" format
4. Save to `data/raw/ffiec/`

### Step 3: Download and Parse Data Dictionary

```bash
# Download MDRM.zip from Federal Reserve
python 02_download_dictionary.py

# Parse for Call Report variables
python 03_parse_dictionary.py
```

Creates `data/dictionary/data_dictionary.parquet` with variable descriptions.

### Step 4: Parse Data Files

```bash
# Parse Chicago Fed data (auto-extracts ZIPs, separates by entity type)
python 04_parse_chicago.py

# Parse FFIEC bulk downloads
python 05_parse_ffiec.py
```

### Step 5: Verify and Summarize

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
| 1 | Commercial Bank | `FFIEC_031_041/` |
| 10, 11 | Foreign Bank Branch | `FFIEC_002/` |
| 13, 17 | Edge/Agreement Corp | `FRB_2886b/` |
