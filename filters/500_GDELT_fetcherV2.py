import os
import pandas as pd

# Set up directories
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
csv_directory = os.path.join(SCRIPT_DIR, "../zips")
output_directory = os.path.join(SCRIPT_DIR, "../company_outputs")
os.makedirs(output_directory, exist_ok=True)
enriched_keywords_file = os.path.join(SCRIPT_DIR, "../enriched_keywords.txt")

# Define GDELT CSV headers
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
columns_to_keep = ['SQLDATE', 'AvgTone']

# Load enriched keywords for each company.
# Each line format: CompanyName:TICKER:keyword1:keyword2:...:keywordN
company_keywords = {}
with open(enriched_keywords_file, 'r', encoding='utf-8') as f:
    for line in f:
        parts = line.strip().split(':')
        if len(parts) >= 2:
            # First part is company name, second is ticker, rest are keywords.
            ticker = parts[1].strip()
            keywords = [p.strip() for p in parts[2:] if p.strip()]  # leave them as strings
            company_keywords[ticker] = keywords

# Process each GDELT CSV file
files = sorted(f for f in os.listdir(csv_directory) if f.endswith(".CSV"))
for file in files:
    file_path = os.path.join(csv_directory, file)
    print(f"Processing file: {file_path}")
    try:
        df = pd.read_csv(file_path, sep='\t', names=headers, on_bad_lines='skip', encoding='utf-8', low_memory=False)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        continue

    # Create a combined text column from Actor1Name and Actor2Name for easier matching
    df['combined'] = df['Actor1Name'].fillna('') + ' ' + df['Actor2Name'].fillna('')
    df['combined'] = df['combined'].str.lower()

    # Loop through each company based on enriched keywords
    for ticker, keywords in company_keywords.items():
        # Create a mask: row is selected if any keyword is found in the combined text
        mask = df['combined'].apply(lambda text: any(keyword.lower() in text for keyword in keywords))
        filtered = df[mask]
        if not filtered.empty:
            # Select only the columns we need
            result = filtered[columns_to_keep].copy()
            output_file = os.path.join(output_directory, f"{ticker}_news.csv")
            # If file doesn't exist, write header; else, append
            if not os.path.exists(output_file):
                result.to_csv(output_file, index=False)
            else:
                result.to_csv(output_file, mode='a', index=False, header=False)
            print(f"Appended {len(result)} rows for {ticker} from {file}")
    # Clean up
    del df['combined']

print("Processing completed.")

