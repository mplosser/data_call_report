"""
04_parse_chicago.py

Extracts Chicago Fed Call Report data and separates by entity type:
- FFIEC_031_041/ - Commercial Banks (FFIEC 031/041) - ONLY for 1976-2010
- FFIEC_002/ - Foreign Bank Branches (FFIEC 002)
- FRB_2886b/ - Edge/Agreement Corporations (FR 2886b)

Handles two types of Chicago Fed data:
- Historical Call Reports (1976-2010): All entity types (FFIEC_031_041, FFIEC_002, FRB_2886b)
- Structure Data (2011-2021Q2): FFIEC_002 and FRB_2886b ONLY
  (FFIEC_031_041 is excluded for 2011+ as it has only ~194 variables vs ~1,000 in FFIEC CDR)

Variable Descriptions:
- If data_dictionary.parquet exists, adds MDRM variable descriptions as
  parquet column metadata (similar to Stata variable labels)

ZIP Extraction:
- Automatically extracts ZIP files to {input-dir}/extracted/
- Original ZIP files are preserved
- Use 07_cleanup.py to delete extracted files and free disk space

Usage:
    # Parse all Chicago Fed data (auto-extracts ZIPs)
    python 04_parse_chicago.py

    # Parse specific date range
    python 04_parse_chicago.py --start-date 2000-01-01 --end-date 2021-12-31

    # Force re-processing of existing files
    python 04_parse_chicago.py --force
"""

import pandas as pd
import pyreadstat
import pyarrow as pa
import pyarrow.parquet as pq
import argparse
import re
from pathlib import Path
from tqdm import tqdm
import warnings
import zipfile
from collections import defaultdict

warnings.filterwarnings('ignore')

# Global dictionary cache (loaded once, used for all files)
VARIABLE_DESCRIPTIONS = {}


def load_data_dictionary(dict_path: Path = None) -> dict:
    """Load variable descriptions from data dictionary."""
    global VARIABLE_DESCRIPTIONS

    if VARIABLE_DESCRIPTIONS:
        return VARIABLE_DESCRIPTIONS

    if dict_path is None:
        dict_path = Path('data/dictionary/data_dictionary.parquet')

    if not dict_path.exists():
        return {}

    try:
        df = pd.read_parquet(dict_path)
        VARIABLE_DESCRIPTIONS = dict(zip(df['Variable'], df['Description']))
        return VARIABLE_DESCRIPTIONS
    except Exception as e:
        print(f"[WARN] Could not load data dictionary: {e}")
        return {}


def write_parquet_with_metadata(df: pd.DataFrame, output_path: Path, descriptions: dict):
    """Write DataFrame to parquet with column descriptions as metadata."""
    # Convert to pyarrow table
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Build new schema with descriptions
    new_fields = []
    for field in table.schema:
        col_name = field.name
        desc = descriptions.get(col_name.upper(), '')

        if desc:
            # Add description as field metadata
            new_metadata = {b'description': desc.encode('utf-8')}
            if field.metadata:
                new_metadata.update(field.metadata)
            new_field = pa.field(field.name, field.type, nullable=field.nullable, metadata=new_metadata)
        else:
            new_field = field

        new_fields.append(new_field)

    new_schema = pa.schema(new_fields, metadata=table.schema.metadata)
    new_table = table.cast(new_schema)

    # Write with snappy compression
    pq.write_table(new_table, output_path, compression='snappy')

# Entity type mappings
ENTITY_TYPES = {
    'FFIEC_031_041': {
        'name': 'Commercial Banks (FFIEC 031/041)',
        'rssd9331_values': [1],
        'description': 'Commercial banks filing FFIEC 031/041 call reports'
    },
    'FFIEC_002': {
        'name': 'Foreign Bank Branches (FFIEC 002)',
        'rssd9331_values': [10, 11],
        'description': 'U.S. branches and agencies of foreign banks'
    },
    'FRB_2886b': {
        'name': 'Edge/Agreement Corporations (FR 2886b)',
        'rssd9331_values': [13, 17],
        'description': 'Edge and Agreement corporations'
    }
}


def infer_reporting_period_from_filename(filename):
    """Extract reporting period from Chicago Fed filename."""
    filename_lower = str(filename).lower()
    chicago_match = re.search(r'call?p?(\d{2})(\d{2})', filename_lower)
    if chicago_match:
        year_2digit = int(chicago_match.group(1))
        month = int(chicago_match.group(2))

        if year_2digit >= 76:
            year = 1900 + year_2digit
        else:
            year = 2000 + year_2digit

        if month == 3:
            day = 31
        elif month == 6:
            day = 30
        elif month == 9:
            day = 30
        elif month == 12:
            day = 31
        else:
            return None

        return pd.Timestamp(year=year, month=month, day=day)

    return None


def extract_xpt_from_zip(zip_path):
    """Extract .xpt file from Chicago Fed ZIP archive."""
    zip_path = Path(zip_path)

    with zipfile.ZipFile(zip_path, 'r') as zf:
        xpt_files = [f for f in zf.namelist() if f.lower().endswith('.xpt')]

        if not xpt_files:
            raise ValueError(f"No .xpt file found in {zip_path}")

        xpt_filename = xpt_files[0]
        extract_path = zip_path.parent / xpt_filename

        if not extract_path.exists():
            zf.extract(xpt_filename, zip_path.parent)

        return extract_path


def process_quarter(xpt_file, reporting_period, output_dir, descriptions, force=False):
    """
    Read XPT file, split by entity type, and write parquet files immediately.
    Returns: Dict[entity_type, record_count] - returns 'skipped' for skipped files
    """
    quarter_str = f"{reporting_period.year}Q{reporting_period.quarter}"

    # Check if all output files already exist (skip if not forcing)
    if not force:
        all_exist = True
        for entity_type, config in ENTITY_TYPES.items():
            if entity_type == 'FFIEC_031_041' and reporting_period > pd.Timestamp('2010-12-31'):
                continue
            output_path = output_dir / entity_type / f"{quarter_str}.parquet"
            if not output_path.exists():
                all_exist = False
                break
        if all_exist:
            return 'skipped'

    try:
        # Read SAS XPORT file
        encodings_to_try = [None, 'latin1', 'cp1252', 'iso-8859-1']
        df = None

        for encoding in encodings_to_try:
            try:
                if encoding is None:
                    df, meta = pyreadstat.read_xport(str(xpt_file))
                else:
                    df, meta = pyreadstat.read_xport(str(xpt_file), encoding=encoding)
                break
            except (UnicodeDecodeError, ValueError):
                if encoding == encodings_to_try[-1]:
                    raise
                continue

        if df is None:
            raise ValueError("Could not read file with any encoding")

        # Add metadata
        df['reporting_period'] = reporting_period

        # Ensure RSSD_ID exists
        if 'RSSD9001' in df.columns:
            df['RSSD_ID'] = df['RSSD9001']
        elif 'IDRSSD' in df.columns:
            df['RSSD_ID'] = df['IDRSSD']
        elif 'RSSD_ID' not in df.columns:
            for col in df.columns:
                col_upper = col.upper()
                if col_upper in ['RSSD9999', 'RSSDDATE']:
                    continue
                if 'RSSD' in col_upper:
                    df['RSSD_ID'] = df[col]
                    break

        # Convert to uppercase
        df.columns = df.columns.str.upper()

        # Check if RSSD9331 exists
        if 'RSSD9331' not in df.columns:
            return {}

        results = {}

        # Split by entity type and write immediately
        for entity_type, config in ENTITY_TYPES.items():
            # Skip FFIEC_031_041 for quarters after 2010Q4
            if entity_type == 'FFIEC_031_041' and reporting_period > pd.Timestamp('2010-12-31'):
                continue

            entity_df = df[df['RSSD9331'].isin(config['rssd9331_values'])].copy()

            if len(entity_df) > 0:
                # Move metadata columns to front
                cols = entity_df.columns.tolist()
                metadata_cols = ['REPORTING_PERIOD', 'RSSD_ID']
                other_cols = [c for c in cols if c not in metadata_cols]
                entity_df = entity_df[[c for c in metadata_cols if c in cols] + other_cols]

                # Write immediately with metadata
                entity_output_dir = output_dir / entity_type
                entity_output_dir.mkdir(parents=True, exist_ok=True)
                output_path = entity_output_dir / f"{quarter_str}.parquet"
                write_parquet_with_metadata(entity_df, output_path, descriptions)

                results[entity_type] = len(entity_df)

        return results

    except Exception as e:
        print(f"\n[ERROR] Failed to process {xpt_file.name}: {e}")
        return {}


def main():
    parser = argparse.ArgumentParser(description='Extract Chicago Fed data by entity type')
    parser.add_argument('--input-dir', type=str, default='data/raw/chicago',
                        help='Directory containing raw ZIPs or extracted .xpt files')
    parser.add_argument('--output-dir', type=str, default='data/processed',
                        help='Base output directory')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None,
                        help='End date (YYYY-MM-DD)')
    parser.add_argument('--force', '-f', action='store_true',
                        help='Overwrite existing output files')

    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    # Find XPT files
    extracted_dir = input_dir / 'extracted'
    xpt_files = sorted([p for p in input_dir.glob('*.xpt')] +
                      ([p for p in extracted_dir.glob('*.xpt')] if extracted_dir.exists() else []))

    # Extract ZIPs if present
    zip_files = sorted(input_dir.glob('*.zip'))
    if zip_files:
        extracted_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n[INFO] Found {len(zip_files)} ZIP files in {input_dir}, extracting to {extracted_dir}...")
        for zip_file in tqdm(zip_files, desc="Extracting raw data"):
            try:
                xpt_file = extract_xpt_from_zip(zip_file)
                dest = extracted_dir / xpt_file.name
                if xpt_file.exists() and xpt_file.parent != extracted_dir:
                    xpt_file.replace(dest)
                    xpt_file = dest
                if xpt_file not in xpt_files:
                    xpt_files.append(xpt_file)
                    xpt_files = sorted(xpt_files)
            except Exception as e:
                print(f"[WARN] Failed to extract {zip_file.name}: {e}")

    if not xpt_files:
        print(f"[ERROR] No .xpt files found in {input_dir}")
        return

    # Filter by date
    start_date = pd.Timestamp(args.start_date) if args.start_date else None
    end_date = pd.Timestamp(args.end_date) if args.end_date else None

    filtered_files = []
    for xpt_file in xpt_files:
        reporting_period = infer_reporting_period_from_filename(xpt_file.name)
        if reporting_period is None:
            continue
        if start_date and reporting_period < start_date:
            continue
        if end_date and reporting_period > end_date:
            continue
        filtered_files.append((xpt_file, reporting_period))

    # Sort by date
    filtered_files.sort(key=lambda x: x[1])

    if not filtered_files:
        print(f"[ERROR] No files found in date range")
        return

    # Load data dictionary for variable descriptions
    descriptions = load_data_dictionary()
    if descriptions:
        print(f"[INFO] Loaded {len(descriptions):,} variable descriptions from data dictionary")
    else:
        print(f"[INFO] No data dictionary found - parquet files will not have variable descriptions")
        print(f"       Run 02_download_dictionary.py and 03_parse_dictionary.py to add descriptions")

    print(f"\n{'='*80}")
    print(f"CHICAGO FED DATA EXTRACTION")
    print(f"{'='*80}")
    print(f"Files to process: {len(filtered_files)}")
    print(f"Output structure:")
    for entity_type, config in ENTITY_TYPES.items():
        print(f"  {output_dir / entity_type}/ - {config['name']}")
    print(f"{'='*80}\n")

    # Process files - single pass: read, split, write immediately
    stats = defaultdict(lambda: {'processed': 0, 'total_records': 0, 'skipped': 0})

    for xpt_file, reporting_period in tqdm(filtered_files, desc="Processing quarters"):
        results = process_quarter(xpt_file, reporting_period, output_dir, descriptions, force=args.force)

        if results == 'skipped':
            for entity_type in ENTITY_TYPES:
                stats[entity_type]['skipped'] += 1
        else:
            for entity_type, record_count in results.items():
                stats[entity_type]['processed'] += 1
                stats[entity_type]['total_records'] += record_count

    # Print summary
    print(f"\n{'='*80}")
    print("EXTRACTION COMPLETE")
    print(f"{'='*80}")

    for entity_type, config in ENTITY_TYPES.items():
        if stats[entity_type]['processed'] > 0 or stats[entity_type]['skipped'] > 0:
            print(f"\n{config['name']}:")
            print(f"  Output: {output_dir / entity_type}")
            print(f"  Processed: {stats[entity_type]['processed']} quarters")
            if stats[entity_type]['skipped'] > 0:
                print(f"  Skipped (already exist): {stats[entity_type]['skipped']} quarters")
            print(f"  Total records: {stats[entity_type]['total_records']:,}")

    print(f"\n{'='*80}\n")


if __name__ == '__main__':
    main()
