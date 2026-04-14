import requests
from pdf_endpoint_check import urls, manual_endpoints

download_path = "data/"
def endpoint_downloader(urls : list) -> None:
    for url in urls:
        try:
            response = requests.get(url)
            if response.status_code == 200:  
                print("Success - API is accessible.")
                print(f"{response.url} has content")
                with open(f"{download_path}{response.url.split('/')[-1]}", 'wb') as f:
                    f.write(response.content)
                print(f"PDF downloaded successfully from {response.url} and saved to {download_path}{response.url.split('/')[-1]}")
            else:
                print(f"Failure - API is accessible but PDF url is no longer working: {response.status_code}")
                print(f"Response content: {response.content}")
                print(f"The PDF URL {response.url} is no longer working. Please check the URL and try again.")
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            print(f"Failure - Unable to establish connection: {e}.")
        except Exception as e:
            print(f"Failure - Unknown error occurred: {e}. Unfortunately, this data is only available via manual PDF extraction.")
            [print(f"Manual endpoints are here: {endpoint}") for endpoint in manual_endpoints]

if __name__ == "__main__":
    endpoint_downloader(urls)
    print("PDF endpoint check and download complete. Please review the output messages above for details on any errors that occurred during the process.")
