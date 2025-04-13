import requests
import pandas as pd
from datetime import datetime

# Define search parameters
keywords = "Apple OR AAPL OR 'Tim Cook' OR iPhone OR Macbook"
api_url = "https://api.gdeltproject.org/api/v2/doc/doc"
params = {
    'query': keywords,
    'mode': 'ArtList',
    'sort': 'DateDesc',
    'maxrecords': 250,
    'format': 'json'
}

try:
    # Send request to GDELT API
    response = requests.get(api_url, params=params)
    response.raise_for_status()  # Check for HTTP errors

    # Check if the response is in JSON format
    if response.headers.get('Content-Type') == 'application/json':
        data = response.json()
    else:
        print("Unexpected content type:", response.headers.get('Content-Type'))
        print("Response content:", response.text)
        exit()

    # Extract relevant information
    articles = []
    for article in data.get('articles', []):
        articles.append({
            'title': article.get('title', ''),
            'url': article.get('url', ''),
            'date': article.get('seendate', ''),
            'source': article.get('domain', ''),
            'language': article.get('language', ''),
            'image': article.get('image', ''),
            'social_shares': article.get('socialimage', {}).get('shares', 0)
        })

    # Convert to DataFrame
    df = pd.DataFrame(articles)

    # Filter articles from 2023 to 2025
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 1, 17)
    df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

    # Sort by social shares (popularity)
    df = df.sort_values(by='social_shares', ascending=False)

    # Save to CSV
    df.to_csv('apple_news_2023_2025.csv', index=False)
    print("Data saved to 'apple_news_2023_2025.csv'")

except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
except ValueError as e:
    print(f"JSON decoding failed: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

