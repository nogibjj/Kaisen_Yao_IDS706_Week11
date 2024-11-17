import requests
import os
import io

from zipfile import ZipFile

FILESTORE_PATH = "dbfs:/FileStore/IDS_hwk"


def is_running_on_databricks():
    """Check if the code is running on Databricks"""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def download_and_extract_csv(url):
    """
    Download a CSV file from URL and read its content
    Returns: CSV content if successful, None otherwise
    """
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Downloading CSV file from {url}")
        return response.content.decode("utf-8"), "fifa_countries_audience.csv"
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")
        return None, None


def extract(
    url="""https://github.com/fivethirtyeight/data/blob/master/fifa/fifa_countries_audience.csv?raw=true""",
    directory=FILESTORE_PATH,
    overwrite=True,
):
    """Extract a url to a file path and unzip if it's a zip file"""
    if is_running_on_databricks():
        try:
            import databricks.data

            csv_content, csv_filename = download_and_extract_csv(url)
            if csv_content is not None:
                csv_path = f"{directory}/{csv_filename}"
                dbutils.fs.put(csv_path, csv_content, overwrite)
                return csv_path
        except Exception as e:
            print(f"Error in Databricks environment: {str(e)}")
        return None
    else:
        print("Not running on Databricks")
        return None


if __name__ == "__main__":
    extract()
