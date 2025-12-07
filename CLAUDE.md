# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Call Report Data Acquisition Pipeline for downloading and processing FFIEC banking data (1985-2025). Separates data by entity type, adds MDRM variable descriptions as parquet column metadata, and converts from SAS XPORT (.xpt) and tab-delimited formats to standardized parquet files.

## Common Commands

### Full Pipeline

```bash
# 1. Download Chicago Fed data
python 01_download_data.py --start-year 1985 --end-year 2021

# 2. Download and parse data dictionary
python 02_download_dictionary.py
python 03_parse_dictionary.py

# 3. Parse data to parquet (with variable descriptions)
python 04_parse_chicago.py
python 05_parse_ffiec.py

# 4. Summarize and verify
python 06_summarize.py

# 5. Cleanup (optional)
python 07_cleanup.py --extracted
```

### Date Filtering

```bash
# Process specific date range
python 04_parse_chicago.py --start-date 2000-01-01 --end-date 2010-12-31

# Summarize with date filter
python 06_summarize.py --start-date 2000-01-01 --end-date 2010-12-31
```

## Architecture

### Script Numbering Convention

Scripts are numbered to show pipeline execution order:
- `01_download_data.py`: Download raw data
- `02_download_dictionary.py`: Download MDRM dictionary
- `03_parse_dictionary.py`: Parse dictionary for relevant variables
- `04_parse_chicago.py`: Parse Chicago Fed data with metadata
- `05_parse_ffiec.py`: Parse FFIEC bulk data with metadata
- `06_summarize.py`: Verify/audit parsed data
- `07_cleanup.py`: Remove intermediate files

### Data Dictionary Integration

Variable descriptions are added as parquet column metadata:
- `02_download_dictionary.py`: Downloads MDRM.zip from Federal Reserve
- `03_parse_dictionary.py`: Filters to Call Report mnemonics (RCON, RCFD, RIAD, etc.)
- `04_parse_chicago.py` / `05_parse_ffiec.py`: Use pyarrow to write with field-level metadata

```python
# Reading variable descriptions from parquet
field = pf.schema_arrow.field('RCON2170')
desc = field.metadata.get(b'description', b'').decode('utf-8')
```

### Data Sources and Coverage

**Chicago Fed Historical (1985-2010)**: All entity types
- FFIEC_031_041, FFIEC_002, FRB_2886b

**Chicago Fed Structure Data (2011-2021Q2)**: FFIEC_002 & FRB_2886b only
- FFIEC_031_041 excluded (use FFIEC CDR instead)

**FFIEC CDR (2011-2025)**: Commercial banks only
- FFIEC_031_041

### Entity Type Separation (RSSD9331 Field)

Chicago Fed data contains multiple entity types identified by `RSSD9331`:
- `1`: Commercial Banks → `FFIEC_031_041/`
- `10, 11`: Foreign Bank Branches → `FFIEC_002/`
- `13, 17`: Edge/Agreement Corporations → `FRB_2886b/`
- Other values: Excluded from output

### Single-Pass Processing (04_parse_chicago.py)

Simple architecture:
1. Load data dictionary (once)
2. Read .xpt file
3. Split by entity type (RSSD9331)
4. Write parquet with metadata
5. Free memory, next file

### Output Format

```
data/processed/
├── FFIEC_031_041/
│   ├── 1985Q1.parquet
│   └── ...
├── FFIEC_002/
│   └── ...
└── FRB_2886b/
    └── ...

data/dictionary/
├── MDRM.zip
├── MDRM.csv
├── data_dictionary.csv
└── data_dictionary.parquet
```

**Parquet file structure**:
- Rows: One per filer (RSSD_ID)
- Columns: `RSSD_ID`, `REPORTING_PERIOD`, MDRM codes (uppercase)
- Metadata: Variable descriptions in column metadata
- Compression: Snappy

## File Organization

**Pipeline scripts** (numbered for execution order):
- `01_download_data.py`: Download Chicago Fed ZIP files
- `02_download_dictionary.py`: Download MDRM data dictionary
- `03_parse_dictionary.py`: Parse dictionary for Call Report variables
- `04_parse_chicago.py`: Parse Chicago Fed SAS XPORT files
- `05_parse_ffiec.py`: Parse FFIEC tab-delimited files
- `06_summarize.py`: Generate summary statistics
- `07_cleanup.py`: Delete raw/processed files

**Data directories** (not in repo):
- `data/raw/chicago/`: Downloaded Chicago Fed ZIP files
- `data/raw/ffiec/`: Manually downloaded FFIEC bulk files
- `data/dictionary/`: MDRM dictionary files
- `data/processed/`: Output parquet files

## Important Notes

**When modifying parse scripts**:
- Load dictionary once with `load_data_dictionary()`
- Use `write_parquet_with_metadata()` to include descriptions
- Entity separation via RSSD9331 filtering
- FFIEC_031_041 excluded after 2010Q4 in 04_parse_chicago.py

**When modifying 06_summarize.py**:
- Uses metadata-only reads for speed
- Combined pivot table shows all entity types side-by-side
- Gap detection per entity type (ignores end of series)

**When adding new data sources**:
- Follow numbered script convention
- Use standardized columns: `RSSD_ID`, `REPORTING_PERIOD`, uppercase MDRM codes
- Include dictionary metadata via `write_parquet_with_metadata()`
- Output to entity subdirectory with `{YEAR}Q{QUARTER}.parquet` naming
