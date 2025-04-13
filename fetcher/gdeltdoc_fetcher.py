from gdeltdoc import GdeltDoc, Filters, near, repeat

# Initialize GDELT object

f = Filters(
    start_date="2023-01-01",
    end_date="2025-01-01",
    keyword="Apple Inc"
)

gd = GdeltDoc()

try:
    # Fetch articles matching the filters
    articles = gd.article_search(f)

    # Check if results exist
    if articles is not None and not articles.empty:
        for _, article in articles.iterrows():
            # Use .get() to avoid KeyErrors if fields are missing
            title = article.get('title', 'No Title')
            date = article.get('date', 'No Date')  # Handle missing 'date'
            url = article.get('url', 'No URL')
            print(f"Title: {title}")
            print(f"Date: {date}")
            print(f"URL: {url}")
            print("-" * 50)
    else:
        print("No articles found matching the filters.")
except Exception as e:
    print(f"Error fetching articles: {e}")

