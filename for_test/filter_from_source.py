import pandas as pd
from gdeltdoc import GdeltDoc, Filters

with open("enriched_keywords.txt", "r") as f:
    companies = [line.strip().split(":") for line in f]

for company in companies:
    name, ticker, *keywords = company
    f = Filters(
        keyword=" OR ".join(keywords),
        start_date="2023-01-01",
        end_date="2024-12-31"
    )
    gd = GdeltDoc()
    timeline = gd.timeline_search("timelinetone", f)
    timeline.to_csv(f"csv/{name}_sentiment.csv", columns=["date", "avg_tone"])
