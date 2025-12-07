"""
02_download_dictionary.py

Download the MDRM (Micro Data Reference Manual) data dictionary from the
Federal Reserve. This dictionary contains descriptions for all MDRM variable
codes used in Call Reports.

The MDRM.zip file contains MDRM.csv with metadata for all Federal Reserve
reporting variables including RCON, RCFD, RIAD, etc.

Usage:
    python 02_download_dictionary.py

    # Custom output directory
    python 02_download_dictionary.py --output-dir data/dictionary

Next step: Run 03_parse_dictionary.py to filter and process the dictionary.
"""

import argparse
import zipfile
from pathlib import Path
import requests
from tqdm import tqdm

MDRM_URL = "https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip"


def download_mdrm(output_dir: Path) -> Path:
    """Download MDRM.zip and extract MDRM.csv."""
    output_dir.mkdir(parents=True, exist_ok=True)

    zip_path = output_dir / "MDRM.zip"
    csv_path = output_dir / "MDRM.csv"

    # Download if not exists or force refresh
    if not zip_path.exists():
        print(f"Downloading MDRM dictionary from {MDRM_URL}...")
        response = requests.get(MDRM_URL, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))

        with open(zip_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    pbar.update(len(chunk))

        print(f"Downloaded: {zip_path}")
    else:
        print(f"Using existing: {zip_path}")

    # Extract MDRM.csv
    if not csv_path.exists() or zip_path.stat().st_mtime > csv_path.stat().st_mtime:
        print(f"Extracting MDRM.csv...")
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find the CSV file (might be in a subdirectory)
            csv_files = [f for f in zf.namelist() if f.lower().endswith('.csv')]
            if not csv_files:
                raise ValueError("No CSV file found in MDRM.zip")

            # Extract the first CSV found
            csv_name = csv_files[0]
            with zf.open(csv_name) as src, open(csv_path, 'wb') as dst:
                dst.write(src.read())

        print(f"Extracted: {csv_path}")
    else:
        print(f"Using existing: {csv_path}")

    return csv_path


def main():
    parser = argparse.ArgumentParser(
        description="Download MDRM data dictionary from Federal Reserve",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download to default location
  python 02_download_dictionary.py

  # Custom output directory
  python 02_download_dictionary.py --output-dir data/dictionary

Next step: Run 03_parse_dictionary.py to filter and process the dictionary.
"""
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path('data/dictionary'),
        help='Directory to save dictionary files (default: data/dictionary)'
    )

    args = parser.parse_args()

    csv_path = download_mdrm(args.output_dir)

    print(f"\nMDRM dictionary downloaded successfully!")
    print(f"  CSV: {csv_path}")
    print(f"  Size: {csv_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"\nNext step: python 03_parse_dictionary.py")


if __name__ == "__main__":
    main()
