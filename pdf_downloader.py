import requests
from pdf_endpoint_check import urls, manual_endpoints
from pathlib import Path
import logging

download_path = Path("bronze/pdfs/")

#https://claude.ai/chat/93d4f9f6-4be3-4056-aa8d-f22358dd1938 
#implement steps suggested by Claude in the above conversation to create a robust PDF endpoint checker and downloader that can handle various edge cases and errors gracefully, and provide clear logging messages for any issues that arise during the process. This will help to ensure that we can reliably download the necessary PDF data for our analysis, and easily identify and troubleshoot any problems that may occur with the endpoints or the downloading process. By implementing these improvements, we can enhance the overall robustness and reliability of our data processing pipeline, and ensure that we have access to the necessary data for our analysis of TD behavior and potential correlations with other factors such as payments, lobbying activities, and member metadata.
def endpoint_downloader(urls: list) -> None:
    download_path.mkdir(parents=True, exist_ok=True)
    for url in urls:
        filename = url.split('/')[-1]
        dest = download_path / filename
        if dest.exists():
            logging.info(f"Skipping (already downloaded): {filename}")
            continue
        try:
            response = requests.get(url, stream=True, timeout=30)
            if response.status_code == 200:
                with open(dest, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logging.info(f"Downloaded: {filename}")
            else:
                logging.warning(f"Unexpected status {response.status_code} for {url}")
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            logging.error(f"Connection error for {url}: {e}")
        except Exception as e:
            logging.error(f"Unknown error for {url}: {e}")
            for endpoint in manual_endpoints:
                logging.info(f"Manual endpoint available: {endpoint}")
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    endpoint_downloader(urls)
    print("PDF endpoint check and download complete. Please review the output messages above for details on any errors that occurred during the process.")
