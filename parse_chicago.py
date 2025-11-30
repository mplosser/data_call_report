"""
parse_chicago.py

Stage 1: Parse raw data from Chicago Fed SAS XPORT files.
Preserves ALL MDRM codes without filtering or renaming.

Usage:
    python parse_chicago.py --input-dir /data/raw/chicago/extracted --output-dir /data/processed
"""

import pandas as pd
import pyreadstat
import argparse
import re
from pathlib import Path
from tqdm import tqdm
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

warnings.filterwarnings('ignore')


def infer_reporting_period_from_filename(filename):
    """
    Extract reporting period from Chicago Fed filename.

    Examples:
        call9203.xpt -> 1992-03-31
        calp8503.xpt -> 1985-03-31
        CALL0012.xpt -> 2000-12-31

    Returns:
        pd.Timestamp or None
    """
    filename_lower = str(filename).lower()

    # Chicago Fed patterns: call9203.xpt, calp8503.xpt, etc.
    chicago_match = re.search(r'call?p?(\d{2})(\d{2})', filename_lower)
    if chicago_match:
        year_2digit = int(chicago_match.group(1))
        month = int(chicago_match.group(2))

        # Convert 2-digit year to 4-digit year
        # 76-99 = 1976-1999, 00-25 = 2000-2025
        if year_2digit >= 76:
            year = 1900 + year_2digit
        else:
            year = 2000 + year_2digit

        # Last day of the month
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


def extract_raw_quarter(sas_file_path, reporting_period):
    """
    Extract all data from a single SAS XPORT file.

    Args:
        sas_file_path: Path to .xpt file
        reporting_period: pd.Timestamp for the quarter

    Returns:
        DataFrame with ALL columns preserved (MDRM codes as column names)
    """
    try:
        # Read SAS XPORT file - try multiple encodings
        encodings_to_try = [None, 'latin1', 'cp1252', 'iso-8859-1']
        df = None

        for encoding in encodings_to_try:
            try:
                if encoding is None:
                    df, meta = pyreadstat.read_xport(str(sas_file_path))
                else:
                    df, meta = pyreadstat.read_xport(str(sas_file_path), encoding=encoding)
                break  # Success!
            except (UnicodeDecodeError, ValueError):
                if encoding == encodings_to_try[-1]:  # Last attempt
                    raise
                continue

        if df is None:
            raise ValueError("Could not read file with any encoding")

        # Add metadata columns
        df['reporting_period'] = reporting_period

        # Ensure RSSD_ID or IDRSSD column exists
        # Priority order: RSSD9001 (Fed ID), IDRSSD, RSSD_ID
        if 'RSSD9001' in df.columns:
            df['RSSD_ID'] = df['RSSD9001']
        elif 'IDRSSD' in df.columns:
            df['RSSD_ID'] = df['IDRSSD']
        elif 'RSSD_ID' not in df.columns:
            # Last resort: look for other RSSD columns (but skip date columns)
            for col in df.columns:
                col_upper = col.upper()
                # Skip columns that are likely dates (RSSD9999, etc.)
                if col_upper in ['RSSD9999', 'RSSDDATE']:
                    continue
                if 'RSSD' in col_upper:
                    df['RSSD_ID'] = df[col]
                    print(f"[WARN] Using {col} as RSSD_ID for {sas_file_path.name}")
                    break

        # Convert column names to uppercase for consistency
        df.columns = df.columns.str.upper()

        # Move metadata columns to front
        cols = df.columns.tolist()
        metadata_cols = ['REPORTING_PERIOD', 'RSSD_ID']
        other_cols = [c for c in cols if c not in metadata_cols]
        df = df[[c for c in metadata_cols if c in cols] + other_cols]

        return df

    except Exception as e:
        print(f"[ERROR] Failed to read {sas_file_path.name}: {e}")
        return None


def process_file_wrapper(args_tuple):
    """
    Wrapper function for parallel processing.

    Args:
        args_tuple: (file_path_str, output_dir_str, start_date_str, end_date_str)

    Returns:
        Tuple of (status, quarter_str, message)
    """
    file_path_str, output_dir_str, start_date_str, end_date_str = args_tuple

    xpt_file = Path(file_path_str)
    output_dir = Path(output_dir_str)
    start_date = pd.Timestamp(start_date_str) if start_date_str else None
    end_date = pd.Timestamp(end_date_str) if end_date_str else None

    try:
        # Infer reporting period
        reporting_period = infer_reporting_period_from_filename(xpt_file.name)

        if reporting_period is None:
            return ('skipped', None, f"Could not infer date from {xpt_file.name}")

        # Check date filters
        if start_date and reporting_period < start_date:
            return ('skipped', None, f"Before start date: {reporting_period}")
        if end_date and reporting_period > end_date:
            return ('skipped', None, f"After end date: {reporting_period}")

        # Extract data
        df = extract_raw_quarter(xpt_file, reporting_period)

        if df is None:
            return ('error', None, f"Failed to extract data from {xpt_file.name}")

        # Save as parquet
        quarter_str = f"{reporting_period.year}Q{reporting_period.quarter}"
        output_path = output_dir / f"{quarter_str}.parquet"

        df.to_parquet(output_path, index=False, compression='snappy')

        return ('success', quarter_str, f"Processed {len(df)} rows")

    except Exception as e:
        return ('error', None, f"Error processing {xpt_file.name}: {e}")


def main():
    parser = argparse.ArgumentParser(description='Extract raw Chicago Fed data to Parquet')
    parser.add_argument('--input-dir', type=str, required=True,
                        help='Directory containing extracted SAS .xpt files')
    parser.add_argument('--output-dir', type=str, required=True,
                        help='Directory to save raw quarterly parquet files')
    parser.add_argument('--start-date', type=str, default=None,
                        help='Start date (YYYY-MM-DD), optional')
    parser.add_argument('--end-date', type=str, default=None,
                        help='End date (YYYY-MM-DD), optional')
    parser.add_argument('--no-parallel', action='store_true',
                        help='Disable parallel processing (slower but uses less memory)')
    parser.add_argument('--workers', type=int, default=None,
                        help='Number of parallel workers (default: number of CPU cores)')

    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Find all .xpt files
    xpt_files = sorted(input_dir.glob('*.xpt'))

    if not xpt_files:
        print(f"[ERROR] No .xpt files found in {input_dir}")
        return

    # Determine number of workers
    if args.workers:
        num_workers = args.workers
    else:
        num_workers = multiprocessing.cpu_count()

    print(f"\n[INFO] Found {len(xpt_files)} SAS XPORT files")
    if not args.no_parallel:
        print(f"[INFO] Using {num_workers} parallel workers")

    processed_count = 0
    skipped_count = 0
    error_count = 0

    if args.no_parallel:
        # Sequential processing (original logic)
        start_date = pd.Timestamp(args.start_date) if args.start_date else None
        end_date = pd.Timestamp(args.end_date) if args.end_date else None

        for xpt_file in tqdm(xpt_files, desc="Extracting raw data"):
            reporting_period = infer_reporting_period_from_filename(xpt_file.name)

            if reporting_period is None:
                print(f"\n[WARN] Could not infer date from {xpt_file.name}, skipping")
                skipped_count += 1
                continue

            if start_date and reporting_period < start_date:
                skipped_count += 1
                continue
            if end_date and reporting_period > end_date:
                skipped_count += 1
                continue

            df = extract_raw_quarter(xpt_file, reporting_period)

            if df is None:
                error_count += 1
                continue

            quarter_str = f"{reporting_period.year}Q{reporting_period.quarter}"
            output_path = output_dir / f"{quarter_str}.parquet"

            df.to_parquet(output_path, index=False, compression='snappy')
            processed_count += 1
    else:
        # Parallel processing
        print("[INFO] Processing files in parallel...")

        # Prepare arguments for parallel processing
        file_args = [(str(xpt_file), str(output_dir), args.start_date, args.end_date)
                     for xpt_file in xpt_files]

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(process_file_wrapper, args): args for args in file_args}

            completed = 0
            for future in as_completed(futures):
                completed += 1
                if completed % 10 == 0:
                    print(f"  Processed {completed}/{len(xpt_files)} files...")

                status, quarter_str, message = future.result()

                if status == 'success':
                    processed_count += 1
                elif status == 'skipped':
                    skipped_count += 1
                elif status == 'error':
                    print(f"\n[ERROR] {message}")
                    error_count += 1

        print(f"  Completed all {len(xpt_files)} files")

    print(f"\n{'='*60}")
    print(f"EXTRACTION COMPLETE")
    print(f"{'='*60}")
    print(f"Processed: {processed_count} quarters")
    print(f"Skipped: {skipped_count} files")
    print(f"Errors: {error_count} files")
    print(f"Output directory: {output_dir}")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
