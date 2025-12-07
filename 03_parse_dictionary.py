"""
03_parse_dictionary.py

Parse the MDRM data dictionary to extract variable descriptions for Call Report
variables. Filters to relevant mnemonic prefixes and keeps the most recent
definition for each variable.

The MDRM contains all Federal Reserve reporting variables. For Call Reports,
the relevant prefixes are:
  - RCON: Report of Condition (consolidated, domestic offices only)
  - RCFD: Report of Condition (consolidated, domestic and foreign offices)
  - RIAD: Report of Income (annualized data items)
  - RCFA, RCFN, RCFW, RCOA, RCOW: Other Call Report items

Usage:
    python 03_parse_dictionary.py

    # Custom input/output directories
    python 03_parse_dictionary.py --input-dir data/dictionary --output-dir data/dictionary

Output:
    - data_dictionary.csv: Human-readable CSV with variable descriptions
    - data_dictionary.parquet: Fast-loading parquet for use by parse scripts

Next step: Run 04_parse_chicago.py or 05_parse_ffiec.py to parse data with metadata.
"""

import argparse
import html
import re
from pathlib import Path

import pandas as pd

# MDRM mnemonic prefixes used in Call Reports
CALL_REPORT_PREFIXES = [
    'RCON',  # Report of Condition (domestic offices)
    'RCFD',  # Report of Condition (domestic + foreign offices)
    'RIAD',  # Report of Income (annualized)
    'RCFA',  # Report of Condition (foreign activities)
    'RCFN',  # Report of Condition (foreign, non-consolidated)
    'RCFW',  # Report of Condition (foreign, worldwide)
    'RCOA',  # Report of Condition (other assets)
    'RCOW',  # Report of Condition (other, worldwide)
]


def clean_description(desc: str) -> str:
    """Clean up HTML entities and whitespace in descriptions."""
    if pd.isna(desc) or not isinstance(desc, str):
        return ""

    # Decode HTML entities
    desc = html.unescape(desc)

    # Remove HTML tags
    desc = re.sub(r'<[^>]+>', '', desc)

    # Normalize whitespace
    desc = re.sub(r'\s+', ' ', desc).strip()

    return desc


def parse_dictionary(input_path: Path, output_dir: Path) -> tuple[Path, Path]:
    """
    Parse MDRM.csv and filter to Call Report variables.

    Returns paths to the output CSV and parquet files.
    """
    print(f"Reading MDRM dictionary from {input_path}...")

    # Read MDRM.csv - try different encodings
    # The MDRM.csv has an extra header row "PUBLIC" that needs to be skipped
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(input_path, encoding=encoding, low_memory=False, skiprows=1)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"Could not read {input_path} with any encoding")

    print(f"  Total rows: {len(df):,}")
    print(f"  Columns: {list(df.columns)}")

    # Identify the columns
    # MDRM has: Mnemonic (prefix), Item Code (number), Item Name (description), Start/End Date
    mnemonic_col = None
    item_code_col = None
    desc_col = None
    end_date_col = None

    for col in df.columns:
        col_lower = col.lower()
        if 'mnemonic' in col_lower:
            mnemonic_col = col
        elif 'item' in col_lower and 'code' in col_lower:
            item_code_col = col
        elif 'item' in col_lower and 'name' in col_lower:
            desc_col = col
        elif 'end' in col_lower and 'date' in col_lower:
            end_date_col = col

    if not mnemonic_col:
        for candidate in ['Mnemonic', 'Variable', 'MDRM', 'Code']:
            if candidate in df.columns:
                mnemonic_col = candidate
                break

    if not item_code_col:
        for candidate in ['Item Code', 'ItemCode', 'Code']:
            if candidate in df.columns:
                item_code_col = candidate
                break

    if not desc_col:
        for candidate in ['Item Name', 'ItemName', 'Description', 'Item_Name']:
            if candidate in df.columns:
                desc_col = candidate
                break

    if not mnemonic_col or not desc_col:
        raise ValueError(f"Could not identify mnemonic/description columns. Columns: {list(df.columns)}")

    print(f"  Mnemonic column: {mnemonic_col}")
    print(f"  Item Code column: {item_code_col}")
    print(f"  Description column: {desc_col}")
    print(f"  End date column: {end_date_col}")

    # Filter to Call Report prefixes (Mnemonic contains prefix like RCON, RCFD, etc.)
    mask = df[mnemonic_col].astype(str).isin(CALL_REPORT_PREFIXES)
    df_filtered = df[mask].copy()

    print(f"  Rows matching Call Report prefixes: {len(df_filtered):,}")

    # Create full variable name: Mnemonic + Item Code (e.g., RCON + 2170 = RCON2170)
    if item_code_col:
        df_filtered['_full_variable'] = (
            df_filtered[mnemonic_col].astype(str) +
            df_filtered[item_code_col].astype(str)
        )
    else:
        df_filtered['_full_variable'] = df_filtered[mnemonic_col].astype(str)

    # Parse end date if available
    if end_date_col:
        df_filtered['_end_date'] = pd.to_datetime(df_filtered[end_date_col], errors='coerce', format='mixed')
    else:
        df_filtered['_end_date'] = pd.NaT

    # Keep most recent entry for each full variable (by end date)
    df_filtered = df_filtered.sort_values('_end_date', ascending=False, na_position='first')
    df_filtered = df_filtered.drop_duplicates(subset=['_full_variable'], keep='first')

    print(f"  Unique variables after dedup: {len(df_filtered):,}")

    # Clean descriptions
    df_filtered['Description'] = df_filtered[desc_col].apply(clean_description)

    # Create output dataframe using full variable name
    output_df = pd.DataFrame({
        'Variable': df_filtered['_full_variable'].str.upper(),
        'Description': df_filtered['Description']
    })

    # Remove empty descriptions
    output_df = output_df[output_df['Description'].str.len() > 0]

    print(f"  Variables with descriptions: {len(output_df):,}")

    # Sort by variable name
    output_df = output_df.sort_values('Variable').reset_index(drop=True)

    # Show prefix breakdown
    print(f"\n  Variables by prefix:")
    for prefix in CALL_REPORT_PREFIXES:
        count = output_df['Variable'].str.startswith(prefix).sum()
        if count > 0:
            print(f"    {prefix}: {count:,}")

    # Save outputs
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / 'data_dictionary.csv'
    parquet_path = output_dir / 'data_dictionary.parquet'

    output_df.to_csv(csv_path, index=False)
    output_df.to_parquet(parquet_path, index=False)

    print(f"\n  Saved: {csv_path}")
    print(f"  Saved: {parquet_path}")

    return csv_path, parquet_path


def main():
    parser = argparse.ArgumentParser(
        description="Parse MDRM dictionary for Call Report variables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Parse with defaults
  python 03_parse_dictionary.py

  # Custom directories
  python 03_parse_dictionary.py --input-dir data/dictionary --output-dir data/dictionary

Next step: Run 04_parse_chicago.py or 05_parse_ffiec.py to parse data with metadata.
"""
    )
    parser.add_argument(
        '--input-dir',
        type=Path,
        default=Path('data/dictionary'),
        help='Directory containing MDRM.csv (default: data/dictionary)'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('data/dictionary'),
        help='Directory for output files (default: data/dictionary)'
    )

    args = parser.parse_args()

    input_path = args.input_dir / 'MDRM.csv'

    if not input_path.exists():
        print(f"Error: {input_path} not found.")
        print(f"Run 02_download_dictionary.py first to download the MDRM dictionary.")
        return 1

    csv_path, parquet_path = parse_dictionary(input_path, args.output_dir)

    print(f"\nData dictionary created successfully!")
    print(f"\nNext step: python 04_parse_chicago.py or python 05_parse_ffiec.py")

    return 0


if __name__ == "__main__":
    exit(main())
