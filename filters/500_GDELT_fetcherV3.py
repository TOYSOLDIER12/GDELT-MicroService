import os
import pandas as pd

# ---------- Configuration ----------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
csv_directory = os.path.join(SCRIPT_DIR, "../zips")
output_directory = os.path.join(SCRIPT_DIR, "../company_outputs")
os.makedirs(output_directory, exist_ok=True)

enriched_keywords_file = os.path.join(SCRIPT_DIR, "../enriched_keywords.txt")
last_week_file = os.path.join(SCRIPT_DIR, "../last_processed_week.txt")

# Define GDELT CSV headers (as provided)
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
columns_to_keep = ['SQLDATE', 'AvgTone']  # We only need these for aggregation

# ---------- Utility Functions ----------
def load_enriched_keywords(file_path):
    """
    Load enriched keywords from file.
    Each line: CompanyName:TICKER:keyword1:keyword2:...:keywordN
    Returns a dictionary mapping ticker to a dict with 'company' and 'keywords'.
    """
    enriched = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split(':')
            if len(parts) >= 2:
                company = parts[0].strip()
                ticker = parts[1].strip()
                keywords = [p.strip() for p in parts[2:] if p.strip()]
                enriched[ticker] = {'company': company, 'keywords': keywords}
    return enriched

def get_last_processed_week():
    """Return the last processed week (as a string YYYYMMDD) from file, or default if missing."""
    if os.path.exists(last_week_file):
        with open(last_week_file, 'r') as f:
            return f.read().strip()
    return "00000000"  # Process all files initially

def update_last_processed_week(week):
    with open(last_week_file, "w") as f:
        f.write(week)

def process_csv_file(file_path):
    """Load a single GDELT CSV file with forced headers."""
    try:
        df = pd.read_csv(file_path, sep='\t', names=headers, on_bad_lines='skip', encoding='utf-8', low_memory=False)
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

# ---------- Main Processing ----------
def process_new_files(enriched, last_week):
    # List and filter CSV files based on filename date (YYYYMMDD at start)
    files = sorted(f for f in os.listdir(csv_directory) if f.endswith(".CSV"))
    new_files = [f for f in files if f[:8] > last_week]
    if not new_files:
        print("No new files to process.")
        return last_week

    # Dictionary to accumulate DataFrames per company (by ticker)
    company_data = {ticker: [] for ticker in enriched.keys()}
    max_file_date = last_week  # To update last processed week

    # Process each new file
    for file in new_files:
        file_date = file[:8]  # Extract date from filename
        if file_date > max_file_date:
            max_file_date = file_date
        file_path = os.path.join(csv_directory, file)
        print(f"Processing file: {file_path}")
        df = process_csv_file(file_path)
        if df.empty:
            continue

        # Create a combined text field for matching
        df['combined'] = (df['Actor1Name'].fillna('') + " " + df['Actor2Name'].fillna('')).str.lower()

        # For each company, filter rows where any enriched keyword appears in the combined text
        for ticker, info in enriched.items():
            keywords = info['keywords']
            mask = df['combined'].apply(lambda text: any(kw.lower() in text for kw in keywords))
            filtered = df[mask]
            if not filtered.empty:
                # Keep only columns needed for aggregation
                filtered = filtered[columns_to_keep].copy()
                company_data[ticker].append(filtered)

    # For each company, if any data was collected, aggregate weekly
    for ticker, dfs in company_data.items():
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            # Convert SQLDATE to datetime (assuming it's in YYYYMMDD format)
            combined_df['SQLDATE'] = pd.to_datetime(combined_df['SQLDATE'], format='%Y%m%d', errors='coerce')
            combined_df.dropna(subset=['SQLDATE'], inplace=True)
            combined_df.set_index('SQLDATE', inplace=True)
            # Resample by week (using mean for AvgTone and count for articles)
            agg_df = combined_df.resample('W')['AvgTone'].agg(['mean', 'count']).reset_index()
            agg_df.rename(columns={'mean': 'AvgTone', 'count': 'Count'}, inplace=True)
            agg_df.insert(1, 'Ticker', ticker)
            # Append aggregated data to file for this ticker
            output_file = os.path.join(output_directory, f"weekly_{ticker}_news.csv")
            write_header = not os.path.exists(output_file)
            agg_df.to_csv(output_file, mode='a', index=False, header=write_header)
            print(f"Aggregated data for {ticker} saved to {output_file}")
        else:
            print(f"No new data for {ticker} in this batch.")

    return max_file_date

if __name__ == "__main__":
    enriched = load_enriched_keywords(enriched_keywords_file)
    last_week = get_last_processed_week()
    print(f"Last processed week: {last_week}")
    new_last_week = process_new_files(enriched, last_week)
    update_last_processed_week(new_last_week)
    print(f"Updated last processed week to {new_last_week}")

