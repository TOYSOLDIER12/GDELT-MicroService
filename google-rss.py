import feedparser
import datetime
import urllib.parse

# Function to scrape news headlines from Google News RSS
def scrape_google_news(query, start_date, end_date):
    query = urllib.parse.quote(query)  # Encode query properly
    base_url = f"https://news.google.com/rss/search?q={query}"
    print(f"Fetching data from: {base_url}")  # Debug URL
    feed = feedparser.parse(base_url)

    if not feed.entries:
        print("No entries found. Check if the feed URL is correct or try a simpler query.")
        return []

    headlines = []

    for entry in feed.entries:
        try:
            # Parse and filter by publication date
            published_date = datetime.datetime.strptime(entry.published, "%a, %d %b %Y %H:%M:%S %Z")
            if start_date <= published_date <= end_date:
                headlines.append({
                    "title": entry.title,
                    "published": entry.published
                })
        except Exception as e:
            print(f"Skipping an entry due to error: {e}")

    return headlines

# Usage
query = "stock market"  # Replace with your topic
start_date = datetime.datetime(2023, 1, 1)
end_date = datetime.datetime(2023, 12, 31)

news_data = scrape_google_news(query, start_date, end_date)
if news_data:
    print(f"Fetched {len(news_data)} articles.")
    for article in news_data[:5]:  # Show first 5
        print(f"{article['published']}: {article['title']}")
else:
    print("No articles were fetched. Try modifying the query or checking the source.")

