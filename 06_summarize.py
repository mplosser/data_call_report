"""
06_summarize.py

Summarize the contents of parsed Call Report parquet files.

Shows combined quarterly breakdown by entity type:
- Quarter and date
- Filer counts per entity type (FFIEC 031/041, FFIEC 002, FRB 2886b)
- Variable counts per entity type
- Summary statistics per entity type

Detects gaps in quarterly coverage per entity type (not end of series).

Usage:
    python 06_summarize.py

    # With date filters
    python 06_summarize.py --start-date 2020-01-01 --end-date 2024-12-31

    # Export to CSV
    python 06_summarize.py --output summary.csv

    # Disable parallel processing (slower but uses less memory)
    python 06_summarize.py --no-parallel
"""

import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing


def summarize_parquet_file(file_path, check_null_columns=False, entity_type=None):
    """
    Summarize a single parquet file using metadata (fast).

    Args:
        file_path: Path to parquet file
        check_null_columns: If True, also count columns that are entirely null (slower)
        entity_type: Entity type (e.g., 'FFIEC_031_041', 'FFIEC_002', etc.)

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

        # Count null columns if requested (requires loading full data)
        num_null_columns = 0
        num_non_null_variables = num_variables

        if check_null_columns:
            # Read full dataframe to check for null columns
            df = pd.read_parquet(file_path)

            # Check each column (excluding metadata)
            data_cols = [col for col in df.columns if col not in metadata_cols]
            null_cols = [col for col in data_cols if df[col].isna().all()]
            num_null_columns = len(null_cols)
            num_non_null_variables = num_variables - num_null_columns

        return {
            'entity_type': entity_type if entity_type else 'All',
            'quarter': quarter_str,
            'date': reporting_period,
            'num_banks': num_banks,
            'num_variables': num_variables,
            'num_non_null_vars': num_non_null_variables,
            'num_null_columns': num_null_columns,
            'file_size_mb': file_size_mb,
            'file_path': str(file_path.name)
        }

    except Exception as e:
        print(f"[ERROR] Failed to read {file_path.name}: {e}")
        return None


def summarize_file_wrapper(args_tuple):
    """Wrapper function for parallel processing (needs Path object as string)."""
    file_path_str, check_null_columns, entity_type = args_tuple
    return summarize_parquet_file(Path(file_path_str), check_null_columns, entity_type)


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

    parser.add_argument(
        '--check-null-columns',
        action='store_true',
        help='Check for columns that are entirely null (slower, requires reading full data)'
    )

    args = parser.parse_args()

    # Set up paths
    input_dir = Path(args.input_dir)

    if not input_dir.exists():
        print(f"[ERROR] Directory not found: {input_dir}")
        return

    # Check for entity subdirectories (FFIEC_031_041/, FFIEC_002/, FRB_2886b/)
    entity_dirs = {
        'FFIEC_031_041': input_dir / 'FFIEC_031_041',
        'FFIEC_002': input_dir / 'FFIEC_002',
        'FRB_2886b': input_dir / 'FRB_2886b'
    }

    # Determine if we have entity subdirectories or flat structure
    has_subdirs = any(d.exists() and d.is_dir() for d in entity_dirs.values())

    parquet_files = []
    file_entity_map = {}  # Map file path to entity type

    if has_subdirs:
        # Collect files from subdirectories
        for entity_type, entity_dir in entity_dirs.items():
            if entity_dir.exists():
                entity_files = sorted(entity_dir.glob('*.parquet'))
                for f in entity_files:
                    file_entity_map[str(f)] = entity_type
                parquet_files.extend(entity_files)
    else:
        # Flat structure - all files in main directory
        parquet_files = sorted(input_dir.glob('*.parquet'))
        for f in parquet_files:
            file_entity_map[str(f)] = None

    if not parquet_files:
        print(f"[ERROR] No parquet files found in {input_dir}")
        if has_subdirs:
            print(f"[INFO] Checked subdirectories: {', '.join(entity_dirs.keys())}")
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
    print(f"Structure: {'Entity subdirectories' if has_subdirs else 'Flat directory'}")
    print(f"Files found: {len(parquet_files)}")
    if has_subdirs:
        for entity_type, entity_dir in entity_dirs.items():
            if entity_dir.exists():
                count = len(list(entity_dir.glob('*.parquet')))
                print(f"  {entity_type}: {count} files")
    if args.check_null_columns:
        print(f"Checking for null columns: Yes (slower)")
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
            entity_type = file_entity_map.get(str(file_path))
            summary = summarize_parquet_file(file_path, args.check_null_columns, entity_type)

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

        # Convert Path objects to strings for pickling and add check_null_columns flag + entity_type
        file_args = [(str(f), args.check_null_columns, file_entity_map.get(str(f))) for f in parquet_files]

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_file = {executor.submit(summarize_file_wrapper, fp): fp for fp in file_args}

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

    # Sort by entity_type and date
    summary_df = summary_df.sort_values(['entity_type', 'date'])

    # Print summary table
    if has_subdirs:
        # Create combined pivot table view (like data_fry9)
        pivot_filers = summary_df.pivot(index='quarter', columns='entity_type', values='num_banks')
        pivot_vars = summary_df.pivot(index='quarter', columns='entity_type', values='num_variables')
        dates = summary_df.groupby('quarter')['date'].first()

        # Entity type display names (shorter for table)
        entity_names = {
            'FFIEC_031_041': '031/041',
            'FFIEC_002': '002',
            'FRB_2886b': '2886b'
        }

        print(f"\n{'='*100}")
        print("QUARTERLY BREAKDOWN BY ENTITY TYPE")
        print(f"{'='*100}")
        print(f"{'Quarter':<8} {'Date':<12} {'031/041':>8} {'002':>8} {'2886b':>8} | {'031/041':>7} {'002':>7} {'2886b':>7}")
        print(f"{'':8} {'':12} {'Filers':>8} {'Filers':>8} {'Filers':>8} | {'Vars':>7} {'Vars':>7} {'Vars':>7}")
        print("-" * 8 + " " + "-" * 12 + " " + "-" * 8 + " " + "-" * 8 + " " + "-" * 8 + " | " + "-" * 7 + " " + "-" * 7 + " " + "-" * 7)

        for quarter in sorted(dates.index):
            date_str = dates[quarter].strftime('%Y-%m-%d') if pd.notna(dates[quarter]) else 'Unknown'

            # Get filer counts (use '-' for missing)
            f_031 = f"{int(pivot_filers.loc[quarter, 'FFIEC_031_041']):,}" if 'FFIEC_031_041' in pivot_filers.columns and pd.notna(pivot_filers.loc[quarter, 'FFIEC_031_041']) else "-"
            f_002 = f"{int(pivot_filers.loc[quarter, 'FFIEC_002']):,}" if 'FFIEC_002' in pivot_filers.columns and pd.notna(pivot_filers.loc[quarter, 'FFIEC_002']) else "-"
            f_2886b = f"{int(pivot_filers.loc[quarter, 'FRB_2886b']):,}" if 'FRB_2886b' in pivot_filers.columns and pd.notna(pivot_filers.loc[quarter, 'FRB_2886b']) else "-"

            # Get variable counts
            v_031 = f"{int(pivot_vars.loc[quarter, 'FFIEC_031_041']):,}" if 'FFIEC_031_041' in pivot_vars.columns and pd.notna(pivot_vars.loc[quarter, 'FFIEC_031_041']) else "-"
            v_002 = f"{int(pivot_vars.loc[quarter, 'FFIEC_002']):,}" if 'FFIEC_002' in pivot_vars.columns and pd.notna(pivot_vars.loc[quarter, 'FFIEC_002']) else "-"
            v_2886b = f"{int(pivot_vars.loc[quarter, 'FRB_2886b']):,}" if 'FRB_2886b' in pivot_vars.columns and pd.notna(pivot_vars.loc[quarter, 'FRB_2886b']) else "-"

            print(f"{quarter:<8} {date_str:<12} {f_031:>8} {f_002:>8} {f_2886b:>8} | {v_031:>7} {v_002:>7} {v_2886b:>7}")

    else:
        # Flat structure - single table
        if args.check_null_columns:
            print(f"{'Quarter':<12} {'Date':<12} {'Banks':>8} {'Variables':>10} {'Non-Null':>10} {'Null Cols':>10} {'Size (MB)':>10}")
            print(f"{'-'*12} {'-'*12} {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

            for _, row in summary_df.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d') if pd.notna(row['date']) else 'Unknown'
                print(f"{row['quarter']:<12} {date_str:<12} {row['num_banks']:>8,} "
                      f"{row['num_variables']:>10,} {row['num_non_null_vars']:>10,} "
                      f"{row['num_null_columns']:>10,} {row['file_size_mb']:>10.1f}")
        else:
            print(f"{'Quarter':<12} {'Date':<12} {'Banks':>8} {'Variables':>10} {'Size (MB)':>10}")
            print(f"{'-'*12} {'-'*12} {'-'*8} {'-'*10} {'-'*10}")

            for _, row in summary_df.iterrows():
                date_str = row['date'].strftime('%Y-%m-%d') if pd.notna(row['date']) else 'Unknown'
                print(f"{row['quarter']:<12} {date_str:<12} {row['num_banks']:>8,} "
                      f"{row['num_variables']:>10,} {row['file_size_mb']:>10.1f}")

    # Print overall statistics
    print(f"\n{'='*80}")
    print(f"SUMMARY STATISTICS")
    print(f"{'='*80}")

    if has_subdirs:
        # Per-entity-type breakdown
        print(f"Total files: {len(summary_df)}")
        print(f"Date range: {summary_df['date'].min().strftime('%Y-%m-%d')} to "
              f"{summary_df['date'].max().strftime('%Y-%m-%d')}")

        print(f"\nEntity Type Breakdown:")
        entity_names = {
            'FFIEC_031_041': 'FFIEC 031/041 (Commercial Banks)',
            'FFIEC_002': 'FFIEC 002 (Foreign Bank Branches)',
            'FRB_2886b': 'FR 2886b (Edge/Agreement Corps)'
        }

        for entity_type in ['FFIEC_031_041', 'FFIEC_002', 'FRB_2886b']:
            entity_data = summary_df[summary_df['entity_type'] == entity_type]
            if len(entity_data) > 0:
                print(f"  {entity_names.get(entity_type, entity_type):<40} "
                      f"{len(entity_data):>3} quarters, "
                      f"avg {entity_data['num_banks'].mean():>6.0f} filers, "
                      f"avg {entity_data['num_variables'].mean():>5.0f} vars, "
                      f"{entity_data['file_size_mb'].sum():>6.1f} MB")

        print(f"\nTotal size: {summary_df['file_size_mb'].sum():,.1f} MB")
    else:
        print(f"Total quarters: {len(summary_df)}")
        print(f"Date range: {summary_df['date'].min().strftime('%Y-%m-%d')} to "
              f"{summary_df['date'].max().strftime('%Y-%m-%d')}")
        print(f"Banks (avg): {summary_df['num_banks'].mean():,.0f}")
        print(f"Banks (min): {summary_df['num_banks'].min():,}")
        print(f"Banks (max): {summary_df['num_banks'].max():,}")
        print(f"Variables (avg): {summary_df['num_variables'].mean():,.0f}")
        print(f"Variables (min): {summary_df['num_variables'].min():,}")
        print(f"Variables (max): {summary_df['num_variables'].max():,}")

        if args.check_null_columns:
            print(f"Non-null vars (avg): {summary_df['num_non_null_vars'].mean():,.0f}")
            print(f"Non-null vars (min): {summary_df['num_non_null_vars'].min():,}")
            print(f"Non-null vars (max): {summary_df['num_non_null_vars'].max():,}")
            print(f"Null columns (avg): {summary_df['num_null_columns'].mean():,.0f}")
            print(f"Null columns (min): {summary_df['num_null_columns'].min():,}")
            print(f"Null columns (max): {summary_df['num_null_columns'].max():,}")

        print(f"Total size: {summary_df['file_size_mb'].sum():,.1f} MB")

    print(f"{'='*80}\n")

    # Check for gaps in quarterly coverage (per entity type)
    if has_subdirs and len(summary_df) > 1:
        summary_df['year'] = summary_df['date'].dt.year
        summary_df['quarter_num'] = summary_df['date'].dt.quarter

        all_gaps = {}
        for entity_type in summary_df['entity_type'].unique():
            entity_data = summary_df[summary_df['entity_type'] == entity_type].sort_values('date')

            if len(entity_data) < 2:
                continue

            gaps = []
            for i in range(len(entity_data) - 1):
                current_year = int(entity_data.iloc[i]['year'])
                current_q = int(entity_data.iloc[i]['quarter_num'])
                next_year = int(entity_data.iloc[i + 1]['year'])
                next_q = int(entity_data.iloc[i + 1]['quarter_num'])

                # Calculate expected next quarter
                if current_q == 4:
                    expected_year = current_year + 1
                    expected_q = 1
                else:
                    expected_year = current_year
                    expected_q = current_q + 1

                # Check if there's a gap (not just end of series)
                if next_year != expected_year or next_q != expected_q:
                    gaps.append(f"{expected_year}Q{expected_q}")

            if gaps:
                all_gaps[entity_type] = gaps

        if all_gaps:
            for entity_type, gaps in all_gaps.items():
                print(f"[WARN] {entity_type}: Missing quarters: {', '.join(gaps[:10])}")
                if len(gaps) > 10:
                    print(f"       ... and {len(gaps) - 10} more")
            print()
    elif len(summary_df) > 1:
        # Flat structure - single gap check
        summary_df['year'] = summary_df['date'].dt.year
        summary_df['quarter_num'] = summary_df['date'].dt.quarter
        summary_df = summary_df.sort_values('date')

        gaps = []
        for i in range(len(summary_df) - 1):
            current_year = int(summary_df.iloc[i]['year'])
            current_q = int(summary_df.iloc[i]['quarter_num'])
            next_year = int(summary_df.iloc[i + 1]['year'])
            next_q = int(summary_df.iloc[i + 1]['quarter_num'])

            if current_q == 4:
                expected_year = current_year + 1
                expected_q = 1
            else:
                expected_year = current_year
                expected_q = current_q + 1

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
