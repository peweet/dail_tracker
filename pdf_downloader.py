import logging
from pathlib import Path

import requests

from pdf_endpoint_check import endpoint_checker, manual_endpoints, urls

download_path = Path("data/bronze/pdfs/")
silver_path = Path("data/silver/")
gold_path = Path("data/gold/")

silver_path.mkdir(parents=True, exist_ok=True)
gold_path.mkdir(parents=True, exist_ok=True)
logging.info(f"Download path set to: {download_path}")
logging.info(f"Silver path set to: {silver_path}")
logging.info(f"Gold path set to: {gold_path}")
# https://stackoverflow.com/questions/16694907/download-a-large-file-in-python-with-requests
# https://claude.ai/chat/93d4f9f6-4be3-4056-aa8d-f22358dd1938
# implement steps suggested by Claude in the above conversation to create a robust PDF endpoint checker and downloader that can handle various edge cases and errors gracefully, and provide clear logging messages for any issues that arise during the process. This will help to ensure that we can reliably download the necessary PDF data for our analysis, and easily identify and troubleshoot any problems that may occur with the endpoints or the downloading process. By implementing these improvements, we can enhance the overall robustness and reliability of our data processing pipeline, and ensure that we have access to the necessary data for our analysis of TD behavior and potential correlations with other factors such as payments, lobbying activities, and member metadata.
session = requests.Session()


def endpoint_downloader(urls: list, session=session) -> None:
    download_path.mkdir(parents=True, exist_ok=True)
    for url in urls:
        if "recordAttendanceForTaa" in url:
            destination_dir = download_path / "attendance"
        elif "parliamentaryAllowances" in url:
            destination_dir = download_path / "payments"
        elif "registerOfMembersInterests" in url:
            destination_dir = download_path / "interests"
        else:
            destination_dir = download_path / "other"
        destination_dir.mkdir(parents=True, exist_ok=True)
        # comment: the belo  code assumes that the URL ends with the filename, which is the case for the Oireachtas PDF endpoints,
        # *but it may need to be adapted if we encounter any
        # URLs that do not follow this pattern. In such cases, we may need to implement a more robust method for determining the destination filename, such as parsing the URL or using metadata from the HTTP response to generate a suitable filename for the downloaded PDF.
        destination = destination_dir / url.split("/")[-1]
        if destination.exists():
            logging.info(f"Skipping (already downloaded): {destination}")
            continue
        try:
            response = session.get(url, stream=True, timeout=30)
            # response.raise_for_status()  # Check for HTTP errors
            if response.status_code == 200:
                with open(destination, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logging.info(f"Downloaded: {destination}")
            else:
                logging.warning(f"Unexpected status {response.status_code} for {url}")
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            logging.error(f"Connection error for {url}: {e}")
        except Exception as e:
            logging.error(f"Unknown error for {url}: {e}")
            for endpoint in manual_endpoints:
                logging.info(f"Manual endpoint available: {endpoint}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    endpoint_checker(urls, session=session)
    endpoint_downloader(urls, session=session)
    print(
        "PDF endpoint check and download complete.\n "
        "Please review the output messages above for details \n "
        "non any errors that occurred during the process."
    )
