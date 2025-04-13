import os
import requests
from datetime import datetime, timedelta


base_url = "http://data.gdeltproject.org/events/"
#base_url = "http://data.gdeltproject.org/gdeltv2/"
# Use the same file to track last processed timestamp
last_processed_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../last_downloaded_week.txt")
download_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../zips")
os.makedirs(download_folder, exist_ok=True)
TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"


# ---------- Utility Functions ----------

def get_last_processed_date(file_path):
    """
    Reads the last processed date from the given file.
    Returns a datetime object. If the file doesn't exist or parsing fails, 
    defaults to January 1, 2023 at 00:00:00.
    """
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            date_str = f.read().strip()
            try:
                return datetime.strptime(date_str, "%Y%m%d%H%M%S")
            except Exception as e:
                print(f"Error parsing last processed date: {e}")
    return datetime(2025, 1, 1, 0, 0, 0)

def update_last_processed_date(file_path, dt):
    """
    Updates the file with the last processed date (as YYYYMMDDHHMMSS).
    """
    with open(file_path, "w") as f:
        f.write(dt.strftime(TIMESTAMP_FORMAT))

def download_gdelt_file(base_url, dt, download_folder):
    """
    Attempts to download a GDELT CSV file for the given datetime.
    The file URL is constructed as: {base_url}{YYYYMMDDHHMMSS}.export.CSV
    Saves the file in the download_folder with a filename 'gdelt_{YYYYMMDDHHMMSS}.export.CSV'
    Returns True if downloaded successfully, False otherwise.
    """
    date_str = dt.strftime("%Y%m%d")
    file_url = f"{base_url}{date_str}.export.CSV.zip"
    print(f"Attempting to download: {file_url}")
    
    response = requests.get(file_url)
    if response.status_code == 200:
        timestamp_str = dt.strftime(TIMESTAMP_FORMAT)
        file_name = os.path.join(download_folder, f"{timestamp_str}.export.CSV.zip")
        with open(file_name, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {file_name}")
        return True
    else:
        print(f"File not found for {date_str} (Status code: {response.status_code})")
        return False

def download_new_gdelt_files(base_url, last_processed_file, download_folder, interval_minutes=15):
    """
    Downloads GDELT CSV files starting from the last processed date up to the current time.
    Only files that haven't been downloaded yet are attempted.
    Updates the last_processed_file with the last downloaded timestamp.
    """
    last_date = get_last_processed_date(last_processed_file)
    current_date = last_date
    now = datetime.now()
    
    while current_date <= now:
        download_gdelt_file(base_url, current_date, download_folder)
        current_date += timedelta(days=1)
        now = datetime.now()  # Update 'now' to keep up with real time
        
    # Update the last processed date (set to the last timestamp that was attempted)
    update_last_processed_date(last_processed_file, current_date - timedelta(minutes=interval_minutes))
    print("Download completed up to current time.")

# ---------- Main Script ----------

if __name__ == "__main__":
        
    download_new_gdelt_files(base_url, last_processed_file, download_folder)

