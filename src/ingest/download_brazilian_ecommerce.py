"""
Download the Brazilian E-Commerce Public Dataset by Olist from Kaggle
and copy all CSV files into the project's data/raw/ directory.

Usage (from repo root):

    pip install kagglehub
    python -m src.ingest.download_brazilian_ecommerce

Notes:
- You need internet access.
- For some environments, Kaggle may still require that you have a Kaggle
  account and have accepted the dataset terms. If you see auth/permission
  errors, configure your Kaggle account/API as per Kaggle docs.
"""
# src/ingest/download_brazilian_ecommerce.py
from pathlib import Path
import shutil
import zipfile
import sys

try:
    import kagglehub
except ImportError as e:
    print(
        "ERROR: kagglehub is not installed.\n"
        "Install it with:\n\n"
        "    pip install kagglehub\n"
    )
    raise e


DATASET_ID = "olistbr/brazilian-ecommerce"


def get_project_root() -> Path:
    """
    Assume this file lives in src/ingest/, so project root is two levels up.
    Adjust this if your layout differs.
    """
    return Path(__file__).resolve().parents[2]


def ensure_raw_dir(root: Path) -> Path:
    raw_dir = root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir


def copy_csvs_from_path(src_root: Path, dest_root: Path) -> int:
    """
    Copy all CSVs from src_root (recursively) into dest_root.
    Returns number of files copied.
    """
    csv_files = list(src_root.rglob("*.csv"))
    if not csv_files:
        return 0

    count = 0
    for src_file in csv_files:
        dest_file = dest_root / src_file.name
        print(f"Copying {src_file.name} -> {dest_file}")
        shutil.copy2(src_file, dest_file)
        count += 1
    return count


def extract_zip_if_needed(src_root: Path) -> Path:
    """
    If dataset is delivered as a zip, extract it into a subfolder and
    return the extraction path. Otherwise, return src_root unchanged.
    """
    zip_files = list(src_root.glob("*.zip"))
    if not zip_files:
        return src_root

    # Take the first zip (this dataset ships as one main zip)
    zip_path = zip_files[0]
    extract_dir = src_root / "extracted"
    extract_dir.mkdir(exist_ok=True)

    print(f"Found zip: {zip_path.name}. Extracting to: {extract_dir}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    return extract_dir


def main() -> None:
    project_root = get_project_root()
    raw_dir = ensure_raw_dir(project_root)

    print(f"Project root: {project_root}")
    print(f"Raw data directory: {raw_dir}")

    print(f"\nDownloading Kaggle dataset: {DATASET_ID} ...")
    dataset_path_str = kagglehub.dataset_download(DATASET_ID)
    dataset_path = Path(dataset_path_str)
    print(f"KaggleHub cached dataset at: {dataset_path}")

    # Handle possible zip, then copy CSVs
    source_root = extract_zip_if_needed(dataset_path)

    print("\nCopying CSV files into data/raw ...")
    copied = copy_csvs_from_path(source_root, raw_dir)

    if copied == 0:
        print(
            "\nWARNING: No CSV files were found after download.\n"
            "Check the dataset structure at:\n"
            f"  {dataset_path}\n"
        )
        sys.exit(1)

    print(f"\nDone. {copied} CSV files copied to: {raw_dir}")


if __name__ == "__main__":
    main()
