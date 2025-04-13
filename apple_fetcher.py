import os
import pandas as pd

# Directory containing extracted CSV files
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
csv_directory = os.path.join(SCRIPT_DIR, "zips")
output_file = os.path.join(SCRIPT_DIR, "weekly_apple_news.csv")

# Keywords to filter Apple-related news
apple_keywords = ["Apple", "AAPL", "Tim Cook", "iPhone", "Macbook"]

# Correct headers for the GDELT files (as you fookin' stated)
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

# Columns to keep for sentiment aggregation (we’ll use 'AvgTone' for sentiment score)
columns_to_keep = ['SQLDATE', 'AvgTone']

def extract_weekly_sentiment(data, week_start_date):
    """ Aggregate sentiment data for the week and add the timestamp """
    try:
        # Convert SQLDATE to datetime and drop invalid rows
        data["SQLDATE"] = pd.to_datetime(data["SQLDATE"], format='%Y%m%d', errors='coerce')
        data.dropna(subset=["SQLDATE"], inplace=True)

        # Aggregate sentiment (AvgTone) for the week
        aggregated_data = {
            "Week": week_start_date,
            "AvgTone": data["AvgTone"].mean(),  # Average sentiment score for the week
            "Count": len(data)  # Number of articles for the week
        }
        return pd.DataFrame([aggregated_data])
    except Exception as e:
        print(f"Error aggregating weekly data: {e}")
        return pd.DataFrame()

def process_csv_file(file_path):
    """ Process a single CSV file to filter Apple-related news and retain relevant columns """
    try:
        # Load the CSV file with the correct headers
        print(f"Reading file: {file_path}")
        data = pd.read_csv(file_path, sep='\t', names=headers, on_bad_lines='skip', encoding='utf-8')

        # Filter rows containing Apple-related keywords in Actor1Name or Actor2Name
        filtered_data = data[
            data.apply(lambda row: any(keyword.lower() in str(row).lower() for keyword in apple_keywords), axis=1)
        ]

        # Select relevant columns if the filtered data is not empty
        if not filtered_data.empty:
            return filtered_data[columns_to_keep].dropna()
        else:
            return pd.DataFrame()  # Empty DataFrame if no Apple-related data found
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return pd.DataFrame()

def process_directory(directory, output_file):
    """ Process CSV files in chunks of 7 and write weekly aggregated results incrementally """
    # Ensure output file exists and has the correct header structure
    if not os.path.exists(output_file):
        pd.DataFrame(columns=["Week", "AvgTone", "Count"]).to_csv(output_file, index=False)

    # Sort files in chronological order (based on filename)
    files = sorted(f for f in os.listdir(directory) if f.endswith(".CSV"))
    weekly_batch = []
    week_start_date = None

    for idx, filename in enumerate(files):
        file_path = os.path.join(directory, filename)
        print(f"Processing file: {file_path}")

        # Process the current file
        data = process_csv_file(file_path)
        if not data.empty:
            weekly_batch.append(data)

        # Every 7 files, aggregate and append to the output file
        if (idx + 1) % 7 == 0 or (idx + 1) == len(files):  # End of week or last file
            if weekly_batch:
                combined_data = pd.concat(weekly_batch, ignore_index=True)
                week_start_date = filename[:8]  # Extract week start date from filename (YYYYMMDD)
                weekly_data = extract_weekly_sentiment(combined_data, week_start_date)
                if not weekly_data.empty:
                    weekly_data.to_csv(output_file, mode='a', index=False, header=False)
                    print(f"Appended weekly data for week starting {week_start_date} to {output_file}")

            # Reset batch for the next week
            weekly_batch = []

# Process all files in the directory
process_directory(csv_directory, output_file)

print(f"Weekly aggregated sentiment data saved incrementally to {output_file}")
