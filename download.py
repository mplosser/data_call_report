"""
Download Call Report data from the Chicago Federal Reserve Bank.

This script downloads quarterly Call Report data in SAS XPORT format from the
Chicago Fed. The downloaded ZIP files contain SAS XPORT (.xpt) files which are
automatically extracted by parse_chicago.py.

Data sources:
- Historical Call Reports (1976-2010): All entity types
  https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data

- Structure Data (2011-2021Q2): FFIEC_002 and FRB_2886b only
  https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-structure-data
  Note: Structure data updates ended in 2021Q2

Coverage by year:
- 1976-2010: All entity types (FFIEC 031/041, FFIEC 002, FR 2886b)
- 2011-2021: FFIEC 002 and FR 2886b only (use FFIEC CDR for 031/041)
- 2022+: No data (use FFIEC CDR for 031/041 only)

Entity types:
- Commercial Banks (FFIEC 031/041)
- Foreign Branches (FFIEC 002)
- Edge/Agreement Corps (FR 2886b)

Usage:
    # Download historical data (1985-2010) - all entity types
    python download.py --start-year 1985 --end-year 2010 --output-dir data/raw/chicago

    # Download structure data (2011-2021) - FFIEC 002 & FR 2886b only
    python download.py --start-year 2011 --end-year 2021 --output-dir data/raw/chicago

    # Download both ranges for complete coverage
    python download.py --start-year 1985 --end-year 2021 --output-dir data/raw/chicago

Output:
    ZIP files saved to: {output-dir}/call{YY}{MM}.zip
    Example: call8503.zip (1985 Q1), call1012.zip (2010 Q4), call2106.zip (2021 Q2)

Next step:
    Run parse_chicago.py to extract ZIPs and convert to parquet (separates by entity type):
    python parse_chicago.py --input-dir data/raw/chicago --output-dir data/processed
"""

import requests
import argparse
import sys
from pathlib import Path
from tqdm import tqdm
import pandas as pd

# Chicago Fed download URL base
CHICAGO_FED_BASE_URL = 'https://www.chicagofed.org/-/media/others/banking/financial-institution-reports/commercial-bank-data/'

# File naming pattern: call[YY][MM]-zip.zip
# where YY is 2-digit year (76-10) and MM is month (03, 06, 09, 12)

# Default output directory (relative to repo root)
DEFAULT_OUTPUT_DIR = 'data/raw/chicago'


def download_chicago_fed_data(start_year=1985, end_year=2021, output_dir=DEFAULT_OUTPUT_DIR):
    """
    Download data from Chicago Fed (individual quarterly files).

    Downloads from two sources:
    - Historical Call Reports (1976-2010): All entity types
    - Structure Data (2011-2021Q2): FFIEC_002 and FRB_2886b only

    Args:
        start_year: First year to download (default: 1985)
        end_year: Last year to download (default: 2021)
        output_dir: Directory to save downloaded files

    Returns:
        List of successfully downloaded file paths
    """
    print("\n" + "="*80)
    print("CHICAGO FED DATA DOWNLOAD")
    print("="*80)

    downloads_dir = Path(output_dir)
    downloads_dir.mkdir(parents=True, exist_ok=True)

    # Generate list of quarters to download
    start = f'{start_year}-03-31'
    end = f'{end_year}-12-31'

    # Ensure we don't go beyond Chicago Fed's data range (1976-2021)
    start_dt = max(pd.to_datetime(start), pd.to_datetime('1976-03-31'))
    end_dt = min(pd.to_datetime(end), pd.to_datetime('2021-06-30'))  # Structure data ends 2021Q2

    try:
        quarters = pd.date_range(start=start_dt, end=end_dt, freq='QE')
    except:
        # Fallback for older pandas versions
        quarters = pd.date_range(start=start_dt, end=end_dt, freq='Q')

    print(f"\nDownloading {len(quarters)} quarterly files...")
    print(f"Period: {quarters[0].strftime('%Y-%m-%d')} to {quarters[-1].strftime('%Y-%m-%d')}")
    print(f"Output: {downloads_dir}\n")

    downloaded_files = []
    failed_downloads = []

    for quarter in tqdm(quarters, desc="Downloading quarters"):
        # Generate filename: call[YY][MM]-zip.zip
        # YY is 2-digit year, MM is month (03, 06, 09, 12)
        year_2digit = str(quarter.year)[-2:]
        month = str(quarter.month).zfill(2)
        filename_base = f"call{year_2digit}{month}"
        filename = f"{filename_base}-zip.zip"

        zip_path = downloads_dir / f"{filename_base}.zip"  # Save with simpler name

        # Skip if already downloaded
        if zip_path.exists():
            downloaded_files.append(zip_path)
            continue

        # Construct URL
        url = f"{CHICAGO_FED_BASE_URL}{filename}"

        # Download with retry logic
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))

            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            downloaded_files.append(zip_path)

        except requests.exceptions.RequestException as e:
            failed_downloads.append((filename, str(e)))
            continue

    print(f"\n{'='*80}")
    print("DOWNLOAD COMPLETE")
    print(f"{'='*80}")
    print(f"Successfully downloaded: {len(downloaded_files)} files")

    if failed_downloads:
        print(f"\n⚠️  Failed to download {len(failed_downloads)} files:")
        for filename, error in failed_downloads[:5]:
            print(f"  - {filename}: {error}")
        if len(failed_downloads) > 5:
            print(f"  ... and {len(failed_downloads) - 5} more")
        print(f"\nNote: Some quarters may not be available from Chicago Fed.")

    print(f"\n{'='*80}")
    print("NEXT STEPS")
    print(f"{'='*80}")
    print(f"\nRun parse_chicago.py to extract ZIPs and convert to parquet:")
    print(f"    python parse_chicago.py --input-dir {downloads_dir} --output-dir data/processed")
    print(f"\nNote: parse_chicago.py automatically extracts ZIP files to {downloads_dir / 'extracted'}/\n")

    return downloaded_files


def main():
    parser = argparse.ArgumentParser(
        description='Download Call Report data from Chicago Federal Reserve',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download historical data (1985-2010) - all entity types
  python download.py --start-year 1985 --end-year 2010

  # Download structure data (2011-2021) - FFIEC_002 & FRB_2886b only
  python download.py --start-year 2011 --end-year 2021

  # Download complete range (1985-2021) for full coverage
  python download.py --start-year 1985 --end-year 2021

  # Custom output directory
  python download.py --start-year 1985 --end-year 2021 \\
      --output-dir data/raw/chicago

Notes:
  - Historical data (1976-2010): All entity types (FFIEC 031/041, 002, FR 2886b)
  - Structure data (2011-2021Q2): FFIEC 002 and FR 2886b only
  - Files are downloaded in SAS XPORT format (.xpt) inside ZIP archives
  - After download, use `parse_chicago.py` to convert .xpt files to parquet
        """
    )

    parser.add_argument(
        '--start-year',
        type=int,
        default=1985,
        help='Start year for data download (default: 1985)'
    )

    parser.add_argument(
        '--end-year',
        type=int,
        default=2021,
        help='End year for data download (default: 2021)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f'Directory to save downloaded files (default: {DEFAULT_OUTPUT_DIR})'
    )

    args = parser.parse_args()

    # Validate year range
    if args.start_year < 1976 or args.end_year > 2021:
        print("⚠️  WARNING: Chicago Fed data is only available for 1976-2021")
        print(f"   Requested: {args.start_year}-{args.end_year}")
        print("   Note: 1976-2010 = all entities, 2011-2021 = FFIEC_002 & FRB_2886b only")

        if args.start_year < 1976:
            args.start_year = 1976
            print(f"   Adjusted start year to: {args.start_year}")

        if args.end_year > 2021:
            args.end_year = 2021
            print(f"   Adjusted end year to: {args.end_year}")

    if args.start_year > args.end_year:
        print(f"ERROR: Start year ({args.start_year}) cannot be after end year ({args.end_year})")
        sys.exit(1)

    # Download data
    downloaded_files = download_chicago_fed_data(
        start_year=args.start_year,
        end_year=args.end_year,
        output_dir=args.output_dir
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
