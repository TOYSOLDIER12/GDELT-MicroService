import pandas as pd
import requests
from rapidfuzz import fuzz

def get_wikidata_aliases(company_name):
    """Fetch official aliases from Wikidata"""
    url = f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={company_name}&language=en&format=json"
    try:
        response = requests.get(url).json()
        return [result["display"]["label"] for result in response.get("search", [])]
    except:
        return []

def expand_keywords(row):
    company, ticker = row.split(':')
    base_keywords = [
        company.strip(),
        ticker.strip(),
        company.replace(" ", ""),       # "Honeywell" → "Honeywell"
        company.split()[0],             # First word ("3M" → "3M")
        company.lower(),                # Lowercase
        company.upper()                 # Uppercase
    ]
    
    # Add Wikidata aliases
    base_keywords += get_wikidata_aliases(company.strip())
    
    # Custom enrichment rules
    enhancements = {
        "Apple Inc.": ["iPhone", "MacBook", "Tim Cook"],
        "Microsoft": ["Azure", "Xbox", "Satya Nadella"],
        "Tesla, Inc.": ["Cybertruck", "Gigafactory", "Elon Musk"]
    }
    base_keywords += enhancements.get(company.strip(), [])
    
    # Remove duplicates
    unique_keys = list(set(str(k) for k in base_keywords if k))
    return f"{company.strip()}:{ticker.strip()}:" + ":".join(unique_keys)

# Process file
with open('tickers.txt', 'r') as f:
    lines = [line.strip() for line in f if line.strip()]
    
enriched_data = [expand_keywords(line) for line in lines]

# Save to new file
with open('enriched_keywords.txt', 'w') as f:
    f.write("\n".join(enriched_data))
