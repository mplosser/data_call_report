# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Call Report Data Acquisition Pipeline for downloading and processing FFIEC banking data (1985-2025). Separates data by entity type and converts from SAS XPORT (.xpt) and tab-delimited formats to standardized parquet files.

## Common Commands

### Download and Parse Data

```bash
# Download Chicago Fed data for complete coverage (1985-2021)
# Historical (1985-2010): all entities + Structure (2011-2021): FFIEC_002 & FRB_2886b
python download.py --start-year 1985 --end-year 2021 --output-dir data/raw/chicago

# Parse Chicago Fed data (auto-extracts ZIPs, separates by entity type)
python parse_chicago.py --input-dir data/raw/chicago --output-dir data/processed

# Parse FFIEC bulk downloads (2011-2025, FFIEC_031_041 only)
python parse_ffiec.py --input-dir data/raw/ffiec --output-dir data/processed

# Summarize all parsed data
python summarize.py --input-dir data/processed
```

### Date Filtering and Options

```bash
# Process specific date range
python parse_chicago.py --start-date 2000-01-01 --end-date 2010-12-31

# Control parallelization (summarize.py only)
python summarize.py --no-parallel          # Disable parallel processing
```

### Data Analysis

```bash
# Check for null columns (slower, loads full data)
python summarize.py --input-dir data/processed --check-null-columns

# Export summary to CSV
python summarize.py --input-dir data/processed --output summary.csv

# Filter summary by date
python summarize.py --input-dir data/processed --start-date 2000-01-01 --end-date 2010-12-31
```

## Architecture

### Data Sources and Coverage

**Three-Source Pipeline for Complete Coverage:**

**Chicago Fed Historical (1985-2010)**: 104 quarters, all entity types
- FFIEC_031_041: Commercial Banks (~8,500/quarter, ~2,800+ variables)
- FFIEC_002: Foreign Bank Branches (~400-500/quarter, ~2,800+ variables)
- FRB_2886b: Edge/Agreement Corporations (~100-250/quarter, ~2,800+ variables)

**Chicago Fed Structure Data (2011-2021Q2)**: 42 quarters, FFIEC_002 & FRB_2886b only
- FFIEC_002: Foreign Bank Branches (~370/quarter, ~1,000 variables)
- FRB_2886b: Edge/Agreement Corporations (~65/quarter, ~970 variables)
- Note: Contains FFIEC_031_041 but with far fewer variables (~194 vs ~1,000), so use FFIEC CDR instead

**FFIEC CDR (2011-2025)**: 59 quarters, commercial banks only
- FFIEC_031_041: Commercial Banks (~4,500-5,000/quarter, ~1,000 variables)

**Total Coverage:**
- FFIEC_031_041: 163 quarters (1985Q1-2025Q3)
- FFIEC_002: 146 quarters (1985Q1-2021Q2) - ended
- FRB_2886b: 146 quarters (1985Q1-2021Q2) - ended

### Entity Type Separation (RSSD9331 Field)

Chicago Fed data contains multiple entity types identified by `RSSD9331`:
- `1`: Commercial Banks → `FFIEC_031_041/`
- `10, 11`: Foreign Bank Branches → `FFIEC_002/`
- `13, 17`: Edge/Agreement Corporations → `FRB_2886b/`
- Other values (9, 21, 22, 24): Excluded from output

**Implementation**: `parse_chicago.py` reads the RSSD9331 column and splits the DataFrame by entity type before writing separate subdirectories.

### Single-Pass Processing (parse_chicago.py)

Simple and efficient single-pass architecture:
1. Read .xpt file
2. Split by entity type (using RSSD9331 field)
3. Write parquet file immediately
4. Free memory and move to next file

**Benefits**:
- Low memory usage (doesn't hold all data in memory)
- Simple and fast processing
- No multiprocessing complexity

### Output Format

**Directory structure**:
```
data/processed/
├── FFIEC_031_041/
│   ├── 1985Q1.parquet
│   ├── 2024Q2.parquet
│   └── ...
├── FFIEC_002/
│   ├── 1985Q1.parquet
│   └── ...
└── FRB_2886b/
    ├── 1985Q1.parquet
    └── ...
```

**Parquet file structure**:
- Rows: One per filer (RSSD_ID)
- Columns: `RSSD_ID`, `REPORTING_PERIOD`, followed by MDRM codes (all uppercase)
- Compression: Snappy
- All numeric values preserved as-is (no type coercion beyond pandas defaults)

### Parallel Processing

**parse_ffiec.py**: Supports parallel processing via `ProcessPoolExecutor` with `process_file_wrapper()`. Each quarter file is independent.

**summarize.py**: Supports parallel processing via `ProcessPoolExecutor` with `summarize_file_wrapper()`. Uses metadata-only reads (`pyarrow.parquet.ParquetFile`) for speed unless `--check-null-columns` is specified.

**Control**: `--workers N` or `--no-parallel` flags available on parse_ffiec.py and summarize.py.

### Key Implementation Details

**Filename parsing** (Chicago Fed):
- Pattern: `call{YY}{MM}.zip` (e.g., `call0503.zip` = 2005Q1)
- 2-digit year: 76-99 → 1976-1999, 00-75 → 2000-2075
- Month to quarter: 03→Q1, 06→Q2, 09→Q3, 12→Q4

**Filename parsing** (FFIEC):
- Patterns: `YYYYMMDD` (e.g., `20240630`) or `MMDDYYYY` (e.g., `03312011`)
- Extract year, month, day; map to quarter based on quarter-end dates

**ZIP extraction** (parse_chicago.py):
- Automatically detects ZIP files in input directory
- Extracts to `{input_dir}/extracted/` subdirectory
- Searches both main directory and `extracted/` for .xpt files
- Original ZIP files are preserved
- Use cleanup.py to delete extracted files when disk space is needed

**Encoding handling**:
- Chicago Fed: Tries `None`, `latin1`, `cp1252`, `iso-8859-1` encodings in sequence
- FFIEC: Tries `utf-8`, falls back to `latin-1`

**RSSD_ID identification**:
- Chicago Fed: Looks for `RSSD9001`, `IDRSSD`, or any column with "RSSD" (excluding RSSD9999, RSSDDATE)
- FFIEC: Looks for any column with "RSSD" or "IDRSSD"

## File Organization

**Core scripts** (all support `--help` for detailed options):
- `download.py`: Download Chicago Fed ZIP files
- `parse_chicago.py`: Parse Chicago Fed SAS XPORT files, separate by entity type
- `parse_ffiec.py`: Parse FFIEC tab-delimited files
- `summarize.py`: Generate summary statistics from parquet files

**Data directories** (not in repo, created by scripts):
- `data/raw/chicago/`: Downloaded Chicago Fed ZIP files
- `data/raw/ffiec/`: Manually downloaded FFIEC bulk files
- `data/processed/`: Output parquet files in entity subdirectories

## Important Notes

**When modifying parse_chicago.py**:
- Simple single-pass architecture: read → split by entity → write immediately
- Entity separation happens in `process_quarter()` via RSSD9331 filtering
- FFIEC_031_041 is automatically excluded for quarters after 2010Q4

**When modifying summarize.py**:
- Uses metadata-only reads for speed (no full file load unless `--check-null-columns`)
- Automatically detects entity subdirectories vs flat structure
- Shows combined pivot table with all entity types side-by-side per quarter
- Gap detection per entity type (doesn't warn about end of series, only actual gaps)

**When adding new data sources**:
- Follow the entity subdirectory pattern (FFIEC_031_041, FFIEC_002, FRB_2886b)
- Use standardized column names: `RSSD_ID`, `REPORTING_PERIOD`, uppercase MDRM codes
- Output to parquet with snappy compression
- Filename pattern: `{YEAR}Q{QUARTER}.parquet` (e.g., `2024Q2.parquet`)
