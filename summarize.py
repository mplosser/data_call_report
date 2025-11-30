"""
summarize.py

Summarize the contents of parsed Call Report parquet files.

Shows for each quarter:
- Date (quarter)
- Number of banks
- Number of MDRM variables

Usage:
    python summarize.py --input-dir data/processed

    # With date filters
    python summarize.py --input-dir /data/processed --start-date 2020-01-01 --end-date 2024-12-31

    # Export to CSV
    python summarize.py --input-dir /data/processed --output summary.csv

    # Disable parallel processing (slower but uses less memory)
    python summarize.py --input-dir /data/processed --no-parallel
"""

import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing


def summarize_parquet_file(file_path):
    """
    Summarize a single parquet file using metadata (fast).

    Args:
        file_path: Path to parquet file

    Returns:
        Dictionary with summary statistics
    """
    try:
        # Use pyarrow to read metadata only (much faster than loading full file)
        parquet_file = pq.ParquetFile(file_path)

        # Extract quarter from filename (e.g., "2024Q2.parquet")
        quarter_str = file_path.stem

        # Get number of rows and columns from metadata
        num_rows = parquet_file.metadata.num_rows
        schema = parquet_file.schema_arrow
        column_names = schema.names

        # Count MDRM variables (exclude metadata columns)
        metadata_cols = {'RSSD_ID', 'REPORTING_PERIOD'}
        num_variables = len([col for col in column_names if col not in metadata_cols])

        # Read just the first row to get reporting period
        # This is much faster than reading the entire file
        first_batch = parquet_file.read_row_group(0, columns=['REPORTING_PERIOD'])
        reporting_period_series = first_batch.column('REPORTING_PERIOD').to_pandas()
        reporting_period = pd.to_datetime(reporting_period_series.iloc[0]) if len(reporting_period_series) > 0 else None

        # For num_banks, we need to read the RSSD_ID column and count unique
        # But we can do this efficiently by only reading that column
        rssd_batch = parquet_file.read(columns=['RSSD_ID'])
        num_banks = rssd_batch.column('RSSD_ID').to_pandas().nunique()

        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        return {
            'quarter': quarter_str,
            'date': reporting_period,
            'num_banks': num_banks,
            'num_variables': num_variables,
            'file_size_mb': file_size_mb,
            'file_path': str(file_path.name)
        }

    except Exception as e:
        print(f"[ERROR] Failed to read {file_path.name}: {e}")
        return None


def summarize_file_wrapper(file_path_str):
    """Wrapper function for parallel processing (needs Path object as string)."""
    return summarize_parquet_file(Path(file_path_str))


def main():
    parser = argparse.ArgumentParser(
        description='Summarize parsed Call Report parquet files'
    )

    parser.add_argument(
        '--input-dir',
            type=str,
            default='data/processed',
        help='Directory containing parquet files'
    )

    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='Start date filter (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date filter (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output CSV file path (optional)'
    )

    parser.add_argument(
        '--no-parallel',
        action='store_true',
        help='Disable parallel processing (slower but uses less memory)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='Number of parallel workers (default: number of CPU cores)'
    )

    args = parser.parse_args()

    # Set up paths
    input_dir = Path(args.input_dir)

    if not input_dir.exists():
        print(f"[ERROR] Directory not found: {input_dir}")
        return

    # Find all parquet files
    parquet_files = sorted(input_dir.glob('*.parquet'))

    if not parquet_files:
        print(f"[ERROR] No parquet files found in {input_dir}")
        return

    # Determine number of workers
    if args.workers:
        num_workers = args.workers
    else:
        num_workers = multiprocessing.cpu_count()

    print(f"\n{'='*80}")
    print(f"CALL REPORT DATA SUMMARY")
    print(f"{'='*80}")
    print(f"Directory: {input_dir}")
    print(f"Files found: {len(parquet_files)}")
    if not args.no_parallel:
        print(f"Parallel workers: {num_workers}")
    print(f"{'='*80}\n")

    # Parse date filters
    start_date = pd.Timestamp(args.start_date) if args.start_date else None
    end_date = pd.Timestamp(args.end_date) if args.end_date else None

    # Summarize each file
    summaries = []

    if args.no_parallel:
        # Sequential processing
        print("Processing files sequentially...")
        for file_path in parquet_files:
            summary = summarize_parquet_file(file_path)

            if summary is None:
                continue

            # Apply date filters
            if summary['date']:
                if start_date and summary['date'] < start_date:
                    continue
                if end_date and summary['date'] > end_date:
                    continue

            summaries.append(summary)
    else:
        # Parallel processing
        print(f"Processing files in parallel with {num_workers} workers...")

        # Convert Path objects to strings for pickling
        file_paths_str = [str(f) for f in parquet_files]

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_file = {executor.submit(summarize_file_wrapper, fp): fp for fp in file_paths_str}

            # Collect results as they complete
            completed = 0
            for future in as_completed(future_to_file):
                completed += 1
                if completed % 20 == 0:
                    print(f"  Processed {completed}/{len(parquet_files)} files...")

                summary = future.result()

                if summary is None:
                    continue

                # Apply date filters
                if summary['date']:
                    if start_date and summary['date'] < start_date:
                        continue
                    if end_date and summary['date'] > end_date:
                        continue

                summaries.append(summary)

        print(f"  Completed all {len(parquet_files)} files")

    # Convert to DataFrame for easy display
    summary_df = pd.DataFrame(summaries)

    if summary_df.empty:
        print("[WARN] No data found matching filters")
        return

    # Sort by date
    summary_df = summary_df.sort_values('date')

    # Print summary table
    print(f"{'Quarter':<12} {'Date':<12} {'Banks':>8} {'Variables':>10} {'Size (MB)':>10}")
    print(f"{'-'*12} {'-'*12} {'-'*8} {'-'*10} {'-'*10}")

    for _, row in summary_df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d') if pd.notna(row['date']) else 'Unknown'
        print(f"{row['quarter']:<12} {date_str:<12} {row['num_banks']:>8,} "
              f"{row['num_variables']:>10,} {row['file_size_mb']:>10.1f}")

    # Print overall statistics
    print(f"\n{'='*80}")
    print(f"OVERALL STATISTICS")
    print(f"{'='*80}")
    print(f"Total quarters: {len(summary_df)}")
    print(f"Date range: {summary_df['date'].min().strftime('%Y-%m-%d')} to "
          f"{summary_df['date'].max().strftime('%Y-%m-%d')}")
    print(f"Banks (avg): {summary_df['num_banks'].mean():,.0f}")
    print(f"Banks (min): {summary_df['num_banks'].min():,}")
    print(f"Banks (max): {summary_df['num_banks'].max():,}")
    print(f"Variables (avg): {summary_df['num_variables'].mean():,.0f}")
    print(f"Variables (min): {summary_df['num_variables'].min():,}")
    print(f"Variables (max): {summary_df['num_variables'].max():,}")
    print(f"Total size: {summary_df['file_size_mb'].sum():,.1f} MB")
    print(f"{'='*80}\n")

    # Check for gaps in quarterly coverage
    if len(summary_df) > 1:
        summary_df['year'] = summary_df['date'].dt.year
        summary_df['quarter_num'] = summary_df['date'].dt.quarter

        gaps = []
        for i in range(len(summary_df) - 1):
            current_year = summary_df.iloc[i]['year']
            current_q = summary_df.iloc[i]['quarter_num']
            next_year = summary_df.iloc[i + 1]['year']
            next_q = summary_df.iloc[i + 1]['quarter_num']

            # Calculate expected next quarter
            if current_q == 4:
                expected_year = current_year + 1
                expected_q = 1
            else:
                expected_year = current_year
                expected_q = current_q + 1

            # Check if there's a gap
            if next_year != expected_year or next_q != expected_q:
                gaps.append(f"{expected_year}Q{expected_q}")

        if gaps:
            print(f"[WARN] Missing quarters detected: {', '.join(gaps[:10])}")
            if len(gaps) > 10:
                print(f"       ... and {len(gaps) - 10} more")
            print()

    # Save to CSV if requested
    if args.output:
        output_path = Path(args.output)
        summary_df.to_csv(output_path, index=False)
        print(f"[INFO] Summary saved to: {output_path}\n")


if __name__ == '__main__':
    main()
