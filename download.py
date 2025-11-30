"""
Download historical Call Report data from the Chicago Federal Reserve Bank.

This script downloads quarterly Call Report data in SAS XPORT format from the
Chicago Fed for the period 1985-2000. The downloaded ZIP files contain SAS
XPORT (.xpt) files which should be extracted before running `parse_chicago.py`.

Data source: https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data
Coverage: 1976-2010 (we use 1985-2000)
Format: SAS XPORT (.xpt) files in ZIP archives

Usage:
    # Download full range (1985-2010)
    python download.py --start-year 1985 --end-year 2010 --output-dir data/raw/chicago

    # Download specific date range
    python download.py --start-year 1990 --end-year 2000 --output-dir data/raw/chicago

Output:
    ZIP files saved to: {output-dir}/call{YY}{MM}.zip
    Example: call8503.zip (1985 Q1)

Next step:
    After extracting the ZIP files, run `parse_chicago.py` to convert the .xpt files to parquet
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


def download_chicago_fed_data(start_year=1985, end_year=2010, output_dir=DEFAULT_OUTPUT_DIR):
    """
    Download historical data from Chicago Fed (individual quarterly files).

    Args:
        start_year: First year to download (default: 1985)
        end_year: Last year to download (default: 2010)
        output_dir: Directory to save downloaded files

    Returns:
        List of successfully downloaded file paths
    """
    print("\n" + "="*80)
    print("CHICAGO FED CALL REPORT DATA DOWNLOAD")
    print("="*80)

    downloads_dir = Path(output_dir)
    downloads_dir.mkdir(parents=True, exist_ok=True)

    # Generate list of quarters to download
    start = f'{start_year}-03-31'
    end = f'{end_year}-12-31'

    # Ensure we don't go beyond Chicago Fed's data range (1976-2010)
    start_dt = max(pd.to_datetime(start), pd.to_datetime('1976-03-31'))
    end_dt = min(pd.to_datetime(end), pd.to_datetime('2010-12-31'))

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
    print(f"\n1. Extract the downloaded ZIP files:")
    print(f"   This will unzip all files to extract the SAS XPORT (.xpt) files\n")

    # Create extraction directory path
    extract_dir = downloads_dir / 'extracted'
    print(f"2. Run the extraction script:")
    print(f"   After extracting ZIPs, run parse_chicago.py on the extracted .xpt files:\\")
    print(f"       python parse_chicago.py --input-dir {downloads_dir / 'extracted'} --output-dir data/processed\n")

    print("Note: You need to unzip the downloaded ZIP files to an 'extracted' subdirectory")
    print("      (or modify parse_chicago.py to read from ZIP archives directly).\n")

    return downloaded_files


def main():
    parser = argparse.ArgumentParser(
        description='Download historical Call Report data from Chicago Federal Reserve',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download full range (1985-2010)
    python download.py --start-year 1985 --end-year 2010

  # Download specific range
    python download.py --start-year 1995 --end-year 2005

  # Custom output directory
  python download.py --start-year 1985 --end-year 2000 \\
      --output-dir data/raw/chicago

Notes:
  - Chicago Fed data is available from 1976-2010
  - Files are downloaded in SAS XPORT format (.xpt) inside ZIP archives
    - After extraction, use `parse_chicago.py` to convert .xpt files to parquet
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
        default=2010,
        help='End year for data download (default: 2010)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f'Directory to save downloaded files (default: {DEFAULT_OUTPUT_DIR})'
    )

    args = parser.parse_args()

    # Validate year range
    if args.start_year < 1976 or args.end_year > 2010:
        print("⚠️  WARNING: Chicago Fed data is only available for 1976-2010")
        print(f"   Requested: {args.start_year}-{args.end_year}")

        if args.start_year < 1976:
            args.start_year = 1976
            print(f"   Adjusted start year to: {args.start_year}")

        if args.end_year > 2010:
            args.end_year = 2010
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
