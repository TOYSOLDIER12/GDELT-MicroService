import os
import pandas as pd

# Get the script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Directory containing extracted CSV files from GDELT
csv_directory = os.path.join(SCRIPT_DIR, "../zips")

# Files for tickers and last processed week
tickers_file = os.path.join(SCRIPT_DIR, "../enriched_keywords.txt")  # Each line: TICKER,Company Name
last_week_file = os.path.join(SCRIPT_DIR, "../last_week.txt")

# Output folder for per-company CSVs
output_folder = os.path.join(SCRIPT_DIR, "../company_outputs1")
os.makedirs(output_folder, exist_ok=True)

# Read tickers from tickers.txt and build a list (and dict) of tickers
# Expected format per line: TICKER,Company Name
tickers_dict = {}
with open(tickers_file, "r") as f:
    for line in f:
        parts = line.strip().split(',')
        if len(parts) >= 1:
            ticker = parts[0].strip().upper()
            tickers_dict[ticker] = parts[1].strip() if len(parts) > 1 else ""
# For matching, create a lowercase list of tickers.
tickers_lower = [ticker.lower() for ticker in tickers_dict.keys()]

# Read the last processed week from file, if it exists
if os.path.exists(last_week_file):
    with open(last_week_file, "r") as f:
        last_processed_week = f.read().strip()
else:
    last_processed_week = "00000000"  # A low date to process everything initially

# Correct headers for the GDELT files
headers = [
    'GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate', 'Actor1Code',
    'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode', 'Actor1EthnicCode',
    'Actor1Religion1Code', 'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code',
    'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode',
    'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code', 'Actor2Type1Code',
    'Actor2Type2Code', 'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode',
    'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions', 'NumSources',
    'NumArticles', 'AvgTone', 'Actor1Geo_Type', 'Actor1Geo_FullName', 'Actor1Geo_CountryCode',
    'Actor1Geo_ADM1Code', 'Actor1Geo_ADM2Code', 'Actor1Geo_Lat', 'Actor1Geo_Long',
    'Actor1Geo_FeatureID', 'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode',
    'Actor2Geo_ADM1Code', 'Actor2Geo_ADM2Code', 'Actor2Geo_Lat', 'Actor2Geo_Long',
    'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode',
    'ActionGeo_ADM1Code', 'ActionGeo_ADM2Code', 'ActionGeo_Lat', 'ActionGeo_Long',
    'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL'
]

# We keep only these columns from the input
columns_to_keep = ['SQLDATE', 'AvgTone', 'Ticker']

def assign_ticker(row, tickers_lower):
    """
    Look for any ticker in the row text. Return the first match in uppercase,
    or None if no match.
    """
    row_str = str(row).lower()
    for ticker in tickers_lower:
        if ticker in row_str:
            return ticker.upper()
    return None

def extract_weekly_sentiment(data, week_start_date):
    """Group data by Ticker and compute average sentiment and article count for the week."""
    try:
        # Convert SQLDATE to datetime and drop invalid rows
        data["SQLDATE"] = pd.to_datetime(data["SQLDATE"], format='%Y%m%d', errors='coerce')
        data.dropna(subset=["SQLDATE"], inplace=True)

        # Group by Ticker and aggregate AvgTone and count the articles
        grouped = data.groupby("Ticker").agg({
            "AvgTone": "mean",
            "SQLDATE": "count"
        }).rename(columns={"SQLDATE": "Count"}).reset_index()
        grouped["Week"] = week_start_date
        # Rearranging columns order
        grouped = grouped[["Week", "Ticker", "AvgTone", "Count"]]
        return grouped
    except Exception as e:
        print(f"Error aggregating weekly data: {e}")
        return pd.DataFrame()

def process_csv_file(file_path, tickers_lower):
    """Read a CSV file, filter for rows related to top companies, and assign the correct Ticker."""
    try:
        print(f"Reading file: {file_path}")
        data = pd.read_csv(file_path, sep='\t', names=headers, on_bad_lines='skip', encoding='utf-8')

        # Assign ticker by checking if any of our top tickers appears in the row (any text field)
        data["Ticker"] = data.apply(lambda row: assign_ticker(row, tickers_lower), axis=1)
        filtered_data = data[data["Ticker"].notna()]

        if not filtered_data.empty:
            return filtered_data[columns_to_keep].dropna()
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return pd.DataFrame()

def process_directory(directory, tickers_lower, last_processed_week):
    """Process CSV files week by week, and write each company's aggregated data into its own CSV."""
    # Get all CSV files (assuming filenames start with YYYYMMDD) and sort them
    files = sorted(f for f in os.listdir(directory) if f.endswith(".CSV"))
    
    # Filter files: process only those newer than last_processed_week
    files_to_process = [f for f in files if f[:8] > last_processed_week]
    if not files_to_process:
        print("No new files to process.")
        return last_processed_week  # No updates

    weekly_batch = []
    current_week = None

    for idx, filename in enumerate(files_to_process):
        file_path = os.path.join(directory, filename)
        print(f"Processing file: {file_path}")

        # Determine the week start date from filename (first 8 characters, YYYYMMDD)
        file_week = filename[:8]

        # If we have moved to a new week, process the previous batch
        if current_week is None:
            current_week = file_week
        elif file_week != current_week:
            # Process the batch for the week
            if weekly_batch:
                combined_data = pd.concat(weekly_batch, ignore_index=True)
                weekly_data = extract_weekly_sentiment(combined_data, current_week)
                # For each ticker, write/update its own CSV file
                for _, row in weekly_data.iterrows():
                    ticker = row["Ticker"]
                    output_path = os.path.join(output_folder, f"weekly_{ticker}_news.csv")
                    # Check if file exists to write header or append
                    header = not os.path.exists(output_path)
                    row.to_frame().T.to_csv(output_path, mode='a', index=False, header=header)
                    print(f"Appended data for {ticker} for week {current_week} to {output_path}")
            # Reset weekly batch for new week
            weekly_batch = []
            current_week = file_week

        # Process current file and add to the weekly batch
        data = process_csv_file(file_path, tickers_lower)
        if not data.empty:
            weekly_batch.append(data)

    # Process remaining batch if any after loop
    if weekly_batch and current_week is not None:
        combined_data = pd.concat(weekly_batch, ignore_index=True)
        weekly_data = extract_weekly_sentiment(combined_data, current_week)
        for _, row in weekly_data.iterrows():
            ticker = row["Ticker"]
            output_path = os.path.join(output_folder, f"weekly_{ticker}_news.csv")
            header = not os.path.exists(output_path)
            row.to_frame().T.to_csv(output_path, mode='a', index=False, header=header)
            print(f"Appended data for {ticker} for week {current_week} to {output_path}")

    # Return the most recent week processed for updating the last_week_file
    return current_week

# Run processing and update last processed week
new_last_week = process_directory(csv_directory, tickers_lower, last_processed_week)
if new_last_week:
    with open(last_week_file, "w") as f:
        f.write(new_last_week)
    print(f"Updated last processed week to {new_last_week}")

