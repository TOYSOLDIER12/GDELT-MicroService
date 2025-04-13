import requests
import json
import csv

def fetch_gdelt_summary(query, timespan="2y", country="US", language="eng"):
    base_url = "https://api.gdeltproject.org/api/v2/summary/summary"
    params = {
        "d": "web",            # Web-based news
        "t": "summary",        # Summary mode
        "k": query,            # Keyword
        "ts": timespan,        # Time span (e.g., 2y for 2 years)
        "fsc": country,        # Filter by country (US)
        "fsl": language,       # Filter by language (English)
        "sta": "list",         # List results
        "stc": "yes",          # Include country info
        "sgt": "yes"           # Include sentiment graph
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

def save_to_csv(data, filename="gdelt_summary.csv"):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Title", "URL", "Date", "Sentiment"])
        for article in data.get("data", []):
            writer.writerow([article["title"], article["url"], article["date"], article["sentiment"]])

# Fetch data
query = "stock market"
summary_data = fetch_gdelt_summary(query)

# Save to CSV if data exists
if summary_data:
    save_to_csv(summary_data)
    print(f"Data saved to gdelt_summary.csv")
else:
    print("No data fetched.")

