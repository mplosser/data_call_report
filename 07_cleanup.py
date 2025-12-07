"""
07_cleanup.py

Utility script to clean up downloaded and extracted Call Report data files.

This script provides options to delete:
1. Extracted .xpt files (saves space while keeping original ZIPs)
2. All raw data (both ZIPs and extracted files)
3. Processed parquet files
4. All data (complete cleanup)

Usage:
    # Delete only extracted .xpt files (keeps ZIPs)
    python 07_cleanup.py --extracted

    # Delete all raw data (ZIPs + extracted)
    python 07_cleanup.py --raw

    # Delete processed parquet files
    python 07_cleanup.py --processed

    # Delete everything (raw + processed)
    python 07_cleanup.py --all

    # Dry run to see what would be deleted
    python 07_cleanup.py --extracted --dry-run

    # Specify custom directories
    python 07_cleanup.py --extracted --raw-dir data/raw/chicago
"""

import argparse
import sys
from pathlib import Path
import shutil


def get_file_size_mb(path):
    """Get total size of files in a directory in MB."""
    total_size = 0
    if path.is_file():
        total_size = path.stat().st_size
    elif path.is_dir():
        for item in path.rglob('*'):
            if item.is_file():
                total_size += item.stat().st_size
    return total_size / (1024 * 1024)


def cleanup_extracted(raw_dir, dry_run=False):
    """
    Delete extracted .xpt files.

    Args:
        raw_dir: Path to raw data directory
        dry_run: If True, only show what would be deleted

    Returns:
        Tuple of (files_deleted, total_mb_freed)
    """
    extracted_dir = Path(raw_dir) / 'extracted'

    if not extracted_dir.exists():
        print(f"[INFO] No extracted directory found: {extracted_dir}")
        return 0, 0.0

    # Find all .xpt files
    xpt_files = list(extracted_dir.glob('*.xpt')) + list(extracted_dir.glob('*.XPT'))

    if not xpt_files:
        print(f"[INFO] No .xpt files found in {extracted_dir}")
        return 0, 0.0

    total_size_mb = sum(get_file_size_mb(f) for f in xpt_files)

    print(f"\n{'='*80}")
    print(f"CLEANUP: Extracted .xpt files")
    print(f"{'='*80}")
    print(f"Directory: {extracted_dir}")
    print(f"Files to delete: {len(xpt_files)}")
    print(f"Space to free: {total_size_mb:,.1f} MB")
    print(f"{'='*80}\n")

    if dry_run:
        print("[DRY RUN] Would delete the following files:")
        for f in xpt_files[:10]:
            print(f"  - {f.name} ({get_file_size_mb(f):.1f} MB)")
        if len(xpt_files) > 10:
            print(f"  ... and {len(xpt_files) - 10} more files")
        return len(xpt_files), total_size_mb

    # Delete files
    deleted_count = 0
    for xpt_file in xpt_files:
        try:
            xpt_file.unlink()
            deleted_count += 1
        except Exception as e:
            print(f"[WARN] Could not delete {xpt_file.name}: {e}")

    print(f"✓ Deleted {deleted_count} files, freed {total_size_mb:,.1f} MB")

    # Try to remove empty extracted directory
    try:
        if not any(extracted_dir.iterdir()):
            extracted_dir.rmdir()
            print(f"✓ Removed empty directory: {extracted_dir}")
    except:
        pass

    return deleted_count, total_size_mb


def cleanup_raw(raw_dir, dry_run=False):
    """
    Delete all raw data (ZIPs and extracted files).

    Args:
        raw_dir: Path to raw data directory
        dry_run: If True, only show what would be deleted

    Returns:
        Tuple of (files_deleted, total_mb_freed)
    """
    raw_path = Path(raw_dir)

    if not raw_path.exists():
        print(f"[INFO] Raw directory not found: {raw_path}")
        return 0, 0.0

    # Find all files
    zip_files = list(raw_path.glob('*.zip'))
    extracted_dir = raw_path / 'extracted'

    total_size_mb = sum(get_file_size_mb(f) for f in zip_files)
    if extracted_dir.exists():
        total_size_mb += get_file_size_mb(extracted_dir)

    total_files = len(zip_files)
    if extracted_dir.exists():
        total_files += len(list(extracted_dir.glob('*.*')))

    print(f"\n{'='*80}")
    print(f"CLEANUP: All raw data (ZIPs + extracted)")
    print(f"{'='*80}")
    print(f"Directory: {raw_path}")
    print(f"ZIP files: {len(zip_files)}")
    if extracted_dir.exists():
        print(f"Extracted files: {len(list(extracted_dir.glob('*.*')))}")
    print(f"Total files to delete: {total_files}")
    print(f"Space to free: {total_size_mb:,.1f} MB")
    print(f"{'='*80}\n")

    if dry_run:
        print("[DRY RUN] Would delete the following:")
        print(f"  - {len(zip_files)} ZIP files")
        if extracted_dir.exists():
            print(f"  - {extracted_dir}/ directory and contents")
        return total_files, total_size_mb

    # Delete ZIP files
    deleted_count = 0
    for zip_file in zip_files:
        try:
            zip_file.unlink()
            deleted_count += 1
        except Exception as e:
            print(f"[WARN] Could not delete {zip_file.name}: {e}")

    # Delete extracted directory
    if extracted_dir.exists():
        try:
            shutil.rmtree(extracted_dir)
            print(f"✓ Removed directory: {extracted_dir}")
        except Exception as e:
            print(f"[WARN] Could not delete {extracted_dir}: {e}")

    print(f"✓ Deleted {deleted_count} files, freed {total_size_mb:,.1f} MB")

    return deleted_count, total_size_mb


def cleanup_processed(processed_dir, dry_run=False):
    """
    Delete processed parquet files.

    Args:
        processed_dir: Path to processed data directory
        dry_run: If True, only show what would be deleted

    Returns:
        Tuple of (files_deleted, total_mb_freed)
    """
    processed_path = Path(processed_dir)

    if not processed_path.exists():
        print(f"[INFO] Processed directory not found: {processed_path}")
        return 0, 0.0

    # Find all parquet files (in subdirectories and main dir)
    parquet_files = list(processed_path.rglob('*.parquet'))

    if not parquet_files:
        print(f"[INFO] No parquet files found in {processed_path}")
        return 0, 0.0

    total_size_mb = sum(get_file_size_mb(f) for f in parquet_files)

    print(f"\n{'='*80}")
    print(f"CLEANUP: Processed parquet files")
    print(f"{'='*80}")
    print(f"Directory: {processed_path}")
    print(f"Files to delete: {len(parquet_files)}")
    print(f"Space to free: {total_size_mb:,.1f} MB")
    print(f"{'='*80}\n")

    if dry_run:
        print("[DRY RUN] Would delete the following files:")
        # Group by entity type
        entity_counts = {}
        for f in parquet_files:
            entity_type = f.parent.name if f.parent.name in ['FFIEC_031_041', 'FFIEC_002', 'FRB_2886b'] else 'Other'
            entity_counts[entity_type] = entity_counts.get(entity_type, 0) + 1

        for entity_type, count in entity_counts.items():
            print(f"  - {entity_type}: {count} files")

        return len(parquet_files), total_size_mb

    # Delete files
    deleted_count = 0
    for parquet_file in parquet_files:
        try:
            parquet_file.unlink()
            deleted_count += 1
        except Exception as e:
            print(f"[WARN] Could not delete {parquet_file.name}: {e}")

    # Try to remove empty entity directories
    for entity_dir in ['FFIEC_031_041', 'FFIEC_002', 'FRB_2886b']:
        entity_path = processed_path / entity_dir
        if entity_path.exists():
            try:
                if not any(entity_path.iterdir()):
                    entity_path.rmdir()
                    print(f"✓ Removed empty directory: {entity_path}")
            except:
                pass

    print(f"✓ Deleted {deleted_count} files, freed {total_size_mb:,.1f} MB")

    return deleted_count, total_size_mb


def main():
    parser = argparse.ArgumentParser(
        description='Clean up Call Report data files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Delete only extracted .xpt files (keeps ZIPs)
  python cleanup.py --extracted

  # Delete all raw data (ZIPs + extracted)
  python cleanup.py --raw

  # Delete processed parquet files
  python cleanup.py --processed

  # Delete everything (raw + processed)
  python cleanup.py --all

  # Dry run to see what would be deleted
  python cleanup.py --extracted --dry-run

  # Delete extracted files from custom directory
  python cleanup.py --extracted --raw-dir data/raw/chicago

  # Delete multiple types
  python cleanup.py --extracted --processed
        """
    )

    # Cleanup options
    parser.add_argument(
        '--extracted',
        action='store_true',
        help='Delete extracted .xpt files (keeps ZIPs)'
    )

    parser.add_argument(
        '--raw',
        action='store_true',
        help='Delete all raw data (ZIPs + extracted)'
    )

    parser.add_argument(
        '--processed',
        action='store_true',
        help='Delete processed parquet files'
    )

    parser.add_argument(
        '--all',
        action='store_true',
        help='Delete everything (raw + processed)'
    )

    # Directory options
    parser.add_argument(
        '--raw-dir',
        type=str,
        default='data/raw/chicago',
        help='Raw data directory (default: data/raw/chicago)'
    )

    parser.add_argument(
        '--ffiec-dir',
        type=str,
        default='data/raw/ffiec',
        help='FFIEC raw data directory (default: data/raw/ffiec)'
    )

    parser.add_argument(
        '--processed-dir',
        type=str,
        default='data/processed',
        help='Processed data directory (default: data/processed)'
    )

    # Other options
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be deleted without actually deleting'
    )

    args = parser.parse_args()

    # Check that at least one cleanup option is selected
    if not (args.extracted or args.raw or args.processed or args.all):
        parser.print_help()
        print("\n[ERROR] Please specify at least one cleanup option: --extracted, --raw, --processed, or --all")
        sys.exit(1)

    # Apply --all flag
    if args.all:
        args.raw = True
        args.processed = True

    # Warn about --raw overriding --extracted
    if args.raw and args.extracted:
        print("[INFO] --raw includes extracted files, ignoring --extracted flag")
        args.extracted = False

    print(f"\n{'='*80}")
    print(f"CALL REPORT DATA CLEANUP")
    print(f"{'='*80}")
    if args.dry_run:
        print("[DRY RUN MODE] No files will be deleted")
    print(f"{'='*80}\n")

    total_files = 0
    total_mb = 0.0

    # Execute cleanup operations
    if args.extracted:
        files, mb = cleanup_extracted(args.raw_dir, args.dry_run)
        total_files += files
        total_mb += mb

        # Also check FFIEC directory for extracted files
        files, mb = cleanup_extracted(args.ffiec_dir, args.dry_run)
        total_files += files
        total_mb += mb

    if args.raw:
        # Clean Chicago Fed raw data
        files, mb = cleanup_raw(args.raw_dir, args.dry_run)
        total_files += files
        total_mb += mb

        # Clean FFIEC raw data
        files, mb = cleanup_raw(args.ffiec_dir, args.dry_run)
        total_files += files
        total_mb += mb

    if args.processed:
        files, mb = cleanup_processed(args.processed_dir, args.dry_run)
        total_files += files
        total_mb += mb

    # Print summary
    print(f"\n{'='*80}")
    print(f"CLEANUP SUMMARY")
    print(f"{'='*80}")
    if args.dry_run:
        print(f"Would delete: {total_files} files ({total_mb:,.1f} MB)")
        print(f"\nRun without --dry-run to actually delete files")
    else:
        print(f"Deleted: {total_files} files")
        print(f"Freed: {total_mb:,.1f} MB ({total_mb/1024:,.2f} GB)")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    main()
