"""
05_parse_ffiec.py

Parse FFIEC bulk download tab-delimited files and convert to parquet format.

The bulk download files contain FFIEC 031/041 commercial banks for a single quarter
in tab-delimited format. Output is saved to FFIEC_031_041/ subdirectory.

Variable Descriptions:
- If data_dictionary.parquet exists, adds MDRM variable descriptions as
  parquet column metadata (similar to Stata variable labels)

Usage:
    python 05_parse_ffiec.py

    # Force re-processing of existing files
    python 05_parse_ffiec.py --force

Output structure:
    data/processed/FFIEC_031_041/2024Q2.parquet

"""

import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import re
from datetime import datetime
import zipfile
import io
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Global dictionary cache (loaded once, used for all files)
VARIABLE_DESCRIPTIONS = {}
VARIABLE_FORMS = {}

# MDRM forms for FFIEC 031/041 commercial banks
FFIEC_031_041_FORMS = {'FFIEC 031', 'FFIEC 041', 'FFIEC 032', 'FFIEC 033', 'FFIEC 034', 'FFIEC 051'}

# Columns to always keep regardless of MDRM mapping
METADATA_COLUMNS = {'REPORTING_PERIOD', 'RSSD_ID', 'RSSD9001', 'IDRSSD'}


def load_data_dictionary(dict_path: Path = None) -> tuple[dict, dict]:
    """Load variable descriptions and form mappings from data dictionary."""
    global VARIABLE_DESCRIPTIONS, VARIABLE_FORMS

    if VARIABLE_DESCRIPTIONS:
        return VARIABLE_DESCRIPTIONS, VARIABLE_FORMS

    if dict_path is None:
        dict_path = Path('data/dictionary/data_dictionary.parquet')

    if not dict_path.exists():
        return {}, {}

    try:
        df = pd.read_parquet(dict_path)
        VARIABLE_DESCRIPTIONS = dict(zip(df['Variable'], df['Description']))
        if 'ReportingForms' in df.columns:
            VARIABLE_FORMS = dict(zip(df['Variable'], df['ReportingForms']))
        return VARIABLE_DESCRIPTIONS, VARIABLE_FORMS
    except Exception as e:
        print(f"[WARN] Could not load data dictionary: {e}")
        return {}, {}


def filter_columns(df: pd.DataFrame, form_mapping: dict) -> pd.DataFrame:
    """
    Drop columns that are NOT on the reporting form AND are all null.
    Keep columns if they're on the form OR have any data.
    """
    cols_to_keep = []
    for col in df.columns:
        col_upper = col.upper()

        # Always keep metadata columns
        if col_upper in METADATA_COLUMNS:
            cols_to_keep.append(col)
            continue

        # Check if column is on the reporting form
        on_form = False
        if form_mapping:
            forms_str = form_mapping.get(col_upper, '')
            if forms_str:
                col_forms = set(forms_str.split(','))
                on_form = bool(col_forms & FFIEC_031_041_FORMS)

        # Check if column has any data
        has_data = df[col].notna().any()

        # Keep if on form OR has data
        if on_form or has_data:
            cols_to_keep.append(col)

    return df[cols_to_keep]


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


def extract_quarter_from_filename(filename):
    """
    Extract quarter information from filename.

    Expected formats:
    - FFIEC_20240630.txt -> 2024Q2 (YYYYMMDD)
    - FFIEC CDR Call Bulk All Schedules 03312011.zip -> 2011Q1 (MMDDYYYY)
    - Call_20240630.txt -> 2024Q2

    Args:
        filename: Filename string

    Returns:
        Tuple of (year, quarter, quarter_str) or (None, None, None) if not found
    """
    # Try MMDDYYYY format first (e.g., "03312011")
    match_mmddyyyy = re.search(r'(\d{2})(\d{2})(\d{4})', filename)

    if match_mmddyyyy:
        month = int(match_mmddyyyy.group(1))
        day = int(match_mmddyyyy.group(2))
        year = int(match_mmddyyyy.group(3))

        # Validate it's a reasonable date
        if 1 <= month <= 12 and 1 <= day <= 31 and 1985 <= year <= 2030:
            # Determine quarter from month
            if month == 3 and day == 31:
                quarter = 1
            elif month == 6 and day == 30:
                quarter = 2
            elif month == 9 and day == 30:
                quarter = 3
            elif month == 12 and day == 31:
                quarter = 4
            else:
                # Fallback: determine quarter from month
                quarter = (month - 1) // 3 + 1

            quarter_str = f"{year}Q{quarter}"
            return year, quarter, quarter_str

    # Try YYYYMMDD format (e.g., "20240630")
    match_yyyymmdd = re.search(r'(\d{4})(\d{2})(\d{2})', filename)

    if match_yyyymmdd:
        year = int(match_yyyymmdd.group(1))
        month = int(match_yyyymmdd.group(2))
        day = int(match_yyyymmdd.group(3))

        # Validate it's a reasonable date
        if 1985 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31:
            # Determine quarter from month
            if month == 3 and day == 31:
                quarter = 1
            elif month == 6 and day == 30:
                quarter = 2
            elif month == 9 and day == 30:
                quarter = 3
            elif month == 12 and day == 31:
                quarter = 4
            else:
                # Fallback: determine quarter from month
                quarter = (month - 1) // 3 + 1

            quarter_str = f"{year}Q{quarter}"
            return year, quarter, quarter_str

    return None, None, None


def parse_bulk_file(file_path):
    """
    Parse a bulk download file (tab-delimited or zip containing tab-delimited).

    The FFIEC bulk files can be:
    1. A single tab-delimited text file (.txt or .csv)
    2. A zip file containing multiple tab-delimited files (one per schedule)

    Args:
        file_path: Path to the bulk download file

    Returns:
        DataFrame in wide format with columns: RSSD_ID, REPORTING_PERIOD, and MDRM codes
    """
    file_path = Path(file_path)

    print(f"  Parsing {file_path.name}...")

    # Determine file type
    if file_path.suffix.lower() == '.zip':
        return parse_zip_file(file_path)
    else:
        return parse_text_file(file_path)


def parse_zip_file(zip_path):
    """
    Parse a zip file containing tab-delimited data.

    Args:
        zip_path: Path to zip file

    Returns:
        DataFrame in wide format
    """
    all_data = []

    with zipfile.ZipFile(zip_path, 'r') as zf:
        # List all files in the zip
        file_list = zf.namelist()
        print(f"    Found {len(file_list)} files in zip")

        for filename in file_list:
            if filename.endswith('.txt') or filename.endswith('.csv'):
                print(f"    Reading {filename}...")

                with zf.open(filename) as f:
                    # Read as text, handling different encodings
                    try:
                        content = f.read().decode('utf-8')
                    except UnicodeDecodeError:
                        content = f.read().decode('latin-1')

                    # Parse the content
                    df = parse_text_content(content, filename)

                    if df is not None and not df.empty:
                        all_data.append(df)

    if not all_data:
        return None

    # Merge all dataframes
    # If multiple files, they might have different MDRM codes
    # Merge on RSSD_ID
    result = all_data[0]

    for df in all_data[1:]:
        result = result.merge(df, on='RSSD_ID', how='outer', suffixes=('', '_dup'))

        # Remove duplicate columns
        dup_cols = [c for c in result.columns if c.endswith('_dup')]
        result = result.drop(columns=dup_cols)

    print(f"    Combined: {len(result)} banks, {len(result.columns)-1} columns")

    return result


def parse_text_file(file_path):
    """
    Parse a single tab-delimited text file.

    Args:
        file_path: Path to text file

    Returns:
        DataFrame in wide format
    """
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    return parse_text_content(content, file_path.name)


def parse_text_content(content, source_name):
    """
    Parse tab-delimited content.

    The FFIEC bulk files have different possible formats:
    1. Simple: Each row is a bank, columns are MDRM codes
    2. Complex: Multiple sections, need to identify structure

    Args:
        content: String content of file
        source_name: Name of source file (for logging)

    Returns:
        DataFrame in wide format
    """
    lines = content.strip().split('\n')

    if not lines:
        print(f"    WARNING: {source_name} is empty")
        return None

    # Read as tab-delimited
    # Try to read with pandas
    try:
        df = pd.read_csv(
            io.StringIO(content),
            sep='\t',
            dtype=str,  # Read everything as string initially
            low_memory=False
        )

        print(f"    Read {len(df)} rows, {len(df.columns)} columns")

        # Check if we have expected columns
        # FFIEC files typically have: IDRSSD or RSSD9001, and many MDRM codes

        # Find RSSD ID column
        rssd_col = None
        for col in df.columns:
            col_upper = str(col).upper()
            if 'RSSD' in col_upper or 'IDRSSD' in col_upper or col_upper == 'RSSD9001':
                rssd_col = col
                break

        if rssd_col is None:
            print(f"    WARNING: Could not find RSSD ID column in {source_name}")
            print(f"    Columns: {df.columns[:10].tolist()}...")
            return None

        # Rename to standard name
        df = df.rename(columns={rssd_col: 'RSSD_ID'})

        # Convert RSSD_ID to integer
        df['RSSD_ID'] = pd.to_numeric(df['RSSD_ID'], errors='coerce')

        # Drop rows with invalid RSSD_ID
        df = df.dropna(subset=['RSSD_ID'])
        df['RSSD_ID'] = df['RSSD_ID'].astype(int)

        # Convert numeric columns
        for col in df.columns:
            if col == 'RSSD_ID':
                continue

            # Try to convert to numeric
            df[col] = pd.to_numeric(df[col], errors='ignore')

        print(f"    Parsed {len(df)} banks")

        return df

    except Exception as e:
        print(f"    ERROR parsing {source_name}: {e}")
        import traceback
        traceback.print_exc()
        return None


def convert_to_standard_format(df, reporting_period):
    """
    Convert parsed data to standard format matching API output.

    Args:
        df: DataFrame with RSSD_ID and MDRM columns
        reporting_period: pd.Timestamp for the quarter

    Returns:
        DataFrame with columns: RSSD_ID, REPORTING_PERIOD, and uppercase MDRM codes
    """
    # Add reporting period
    df['REPORTING_PERIOD'] = reporting_period

    # Ensure column names are uppercase
    df.columns = [str(col).upper() for col in df.columns]

    # Reorder: RSSD_ID, REPORTING_PERIOD, then alphabetical MDRM codes
    metadata_cols = ['RSSD_ID', 'REPORTING_PERIOD']
    mdrm_cols = sorted([c for c in df.columns if c not in metadata_cols])

    df = df[metadata_cols + mdrm_cols]

    return df


def process_file_wrapper(args_tuple):
    """
    Wrapper function for parallel processing.

    Args:
        args_tuple: (file_path_str, output_dir_str)

    Returns:
        Tuple of (status, quarter_str, message)
    """
    file_path_str, output_dir_str = args_tuple

    file_path = Path(file_path_str)
    output_dir = Path(output_dir_str)

    try:
        # Extract quarter from filename
        year, quarter, quarter_str = extract_quarter_from_filename(file_path.name)

        if quarter_str is None:
            return ('error', None, f"Could not extract quarter from {file_path.name}")

        # FFIEC data goes into FFIEC_031_041 subdirectory (commercial banks only)
        entity_output_dir = output_dir / 'FFIEC_031_041'
        entity_output_dir.mkdir(parents=True, exist_ok=True)

        # Check if already processed
        output_path = entity_output_dir / f"{quarter_str}.parquet"
        if output_path.exists():
            return ('skipped', quarter_str, "Already exists")

        # Parse the file
        df = parse_bulk_file(file_path)

        if df is None or df.empty:
            return ('error', quarter_str, "No data parsed")

        # Convert to standard format
        reporting_period = pd.Timestamp(year=year, month=quarter*3, day=1)
        df = convert_to_standard_format(df, reporting_period)

        # Save as parquet
        df.to_parquet(output_path, index=False, compression='snappy')

        return ('success', quarter_str, f"{len(df)} banks, {len(df.columns)-2} MDRM codes")

    except Exception as e:
        return ('error', None, f"Error processing {file_path.name}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Parse FFIEC bulk download files to parquet format'
    )

    # Make the input arguments optional and provide sensible defaults so the
    # script can be run without flags when files are placed in the repository
    # layout's default locations.
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        '--input-dir',
        type=str,
        default='data/raw/ffiec',
        help='Directory containing bulk download files (default: data/raw/ffiec)'
    )
    group.add_argument(
        '--input-file',
        type=str,
        help='Single bulk download file to parse'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/processed',
        help='Directory to save parsed parquet files (default: data/processed)'
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Overwrite existing output files'
    )

    args = parser.parse_args()

    # Set up paths
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # FFIEC data goes into FFIEC_031_041 subdirectory (commercial banks only)
    entity_output_dir = output_dir / 'FFIEC_031_041'
    entity_output_dir.mkdir(parents=True, exist_ok=True)

    # Get list of files to process
    if args.input_file:
        files_to_process = [Path(args.input_file)]
    else:
        input_dir = Path(args.input_dir)
        # Look for text, csv, and zip files
        files_to_process = (
            list(input_dir.glob('*.txt')) +
            list(input_dir.glob('*.csv')) +
            list(input_dir.glob('*.zip'))
        )
        files_to_process.sort()

    if not files_to_process:
        print("No files found to process")
        return

    # Load data dictionary for variable descriptions and form mappings
    descriptions, form_mapping = load_data_dictionary()
    if descriptions:
        print(f"[INFO] Loaded {len(descriptions):,} variable descriptions from data dictionary")
        if form_mapping:
            print(f"[INFO] Loaded form mappings for column filtering")
        else:
            print(f"[INFO] No form mappings found - columns will only be filtered by null values")
    else:
        print(f"[INFO] No data dictionary found - parquet files will not have variable descriptions")
        print(f"       Run 02_download_dictionary.py and 03_parse_dictionary.py to add descriptions")

    print(f"\n{'='*60}")
    print(f"FFIEC BULK FILE PARSER")
    print(f"{'='*60}")
    print(f"Files to process: {len(files_to_process)}")
    print(f"Output directory: {entity_output_dir}")
    print(f"{'='*60}\n")

    processed_count = 0
    skipped_count = 0
    error_count = 0

    for file_path in files_to_process:
        # Extract quarter from filename
        year, quarter, quarter_str = extract_quarter_from_filename(file_path.name)

        if quarter_str is None:
            print(f"[{file_path.name}] WARNING: Could not extract quarter from filename")
            error_count += 1
            continue

        # Check if already processed
        output_path = entity_output_dir / f"{quarter_str}.parquet"
        if output_path.exists() and not args.force:
            print(f"[{quarter_str}] Already exists, skipping (use --force to overwrite)")
            skipped_count += 1
            continue

        print(f"[{quarter_str}] Processing {file_path.name}...")

        try:
            # Parse the file
            df = parse_bulk_file(file_path)

            if df is None or df.empty:
                print(f"[{quarter_str}] ERROR: No data parsed")
                error_count += 1
                continue

            # Convert to standard format
            reporting_period = pd.Timestamp(year=year, month=quarter*3, day=1)
            df = convert_to_standard_format(df, reporting_period)

            # Filter columns: only keep those designated for FFIEC 031/041 AND non-null
            original_cols = len(df.columns)
            df = filter_columns(df, form_mapping)
            filtered_cols = len(df.columns)

            # Save as parquet with metadata
            write_parquet_with_metadata(df, output_path, descriptions)

            print(f"[{quarter_str}] Saved: {len(df)} banks, {filtered_cols-2} columns (filtered from {original_cols-2})")
            print(f"[{quarter_str}] Output: {output_path.name}\n")

            processed_count += 1

        except Exception as e:
            print(f"[{quarter_str}] ERROR: {e}")
            import traceback
            traceback.print_exc()
            error_count += 1

    print(f"\n{'='*60}")
    print(f"PARSING COMPLETE")
    print(f"{'='*60}")
    print(f"Processed: {processed_count} files")
    if skipped_count > 0:
        print(f"Skipped (already exist): {skipped_count} files")
    print(f"Errors: {error_count} files")
    print(f"Output directory: {entity_output_dir}")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
