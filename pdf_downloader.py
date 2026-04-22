"""
pdf_downloader.py
-----------------
Downloads all PDFs required by the pipeline into the correct bronze directories.
URLs are the single source of truth in utility/constants.py.
Destinations are resolved from config.py.

Run standalone:  python pdf_downloader.py
Skips files that already exist (idempotent).
"""

import logging
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from utility.constants import ATTENDANCE_PDFS, DAIL_INTERESTS_PDFS, PAYMENT_PDFS, SEANAD_INTERESTS_PDFS
from config import ATTENDANCE_PDF_DIR, PAYMENTS_PDF_DIR, INTERESTS_PDF_DIR
from pdf_endpoint_check import check_all, print_summary, GROUPS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Map each URL group to its bronze destination directory
_DEST: dict[str, Path] = {
    "Attendance":       ATTENDANCE_PDF_DIR,
    "Payments":         PAYMENTS_PDF_DIR,
    "Dáil Interests":   INTERESTS_PDF_DIR,
    "Seanad Interests": INTERESTS_PDF_DIR,
}


def _dest_for(url: str) -> Path:
    """Derive the destination directory from the URL path segment."""
    if "recordAttendanceForTaa" in url:
        return ATTENDANCE_PDF_DIR
    if "parliamentaryAllowances" in url:
        return PAYMENTS_PDF_DIR
    if "registerOfMembersInterests" in url:
        return INTERESTS_PDF_DIR
    return PAYMENTS_PDF_DIR.parent / "other"


def download_all(groups: dict[str, list[str]]) -> dict[str, list[str]]:
    """
    Download every URL in every group into the appropriate bronze directory.
    Skips files that already exist.
    Returns { "downloaded": [...], "skipped": [...], "failed": [...] }
    """
    downloaded, skipped, failed = [], [], []

    for group_name, urls in groups.items():
        dest_dir = _DEST[group_name]
        dest_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"\n── {group_name} → {dest_dir} ──")

        for url in urls:
            filename = url.split("/")[-1]
            destination = dest_dir / filename

            if destination.exists():
                logger.info(f"SKIP (exists) {filename}")
                skipped.append(url)
                continue

            try:
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                with open(destination, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"DOWN {filename}")
                downloaded.append(url)
            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP {url} — {e}")
                failed.append(url)
            except requests.exceptions.ConnectionError as e:
                logger.error(f"CONN {url} — {e}")
                failed.append(url)
            except Exception as e:
                logger.error(f"ERR  {url} — {e}")
                failed.append(url)

    return {"downloaded": downloaded, "skipped": skipped, "failed": failed}


def print_download_summary(result: dict[str, list[str]]) -> None:
    print("\n" + "=" * 60)
    print("PDF DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"  Downloaded : {len(result['downloaded'])}")
    print(f"  Skipped    : {len(result['skipped'])} (already on disk)")
    print(f"  Failed     : {len(result['failed'])}")
    if result["failed"]:
        print("\nFailed URLs:")
        for url in result["failed"]:
            print(f"  {url}")
    print("=" * 60)


if __name__ == "__main__":
    logger.info("Running endpoint check before download…")
    check_results = check_all(GROUPS)
    print_summary(check_results)

    broken = [url for v in check_results.values() for url in v["broken"]]
    if broken:
        logger.warning(f"{len(broken)} broken URL(s) will be skipped during download.")

    logger.info("\nStarting downloads…")
    dl_result = download_all(GROUPS)
    print_download_summary(dl_result)

    sys.exit(1 if dl_result["failed"] else 0)
