import requests

def get_wikidata_aliases(company_name):
    """
    Fetch official aliases from Wikidata for the given company name.
    Returns a list of alias strings.
    """
    url = f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={company_name}&language=en&format=json"
    try:
        response = requests.get(url)
        data = response.json()
        aliases = []
        for result in data.get("search", []):
            # Extract alias from the 'display' object if available
            if "display" in result and "label" in result["display"]:
                aliases.append(result["display"]["label"])
        return aliases
    except Exception as e:
        print(f"Error fetching Wikidata aliases for {company_name}: {e}")
        return []

def expand_keywords(line):
    """
    Given a line in the format "Company Name:Ticker", generate enriched keywords.
    Returns a string: "Company Name:Ticker:kw1:kw2:kw3:..."
    """
    parts = line.strip().split(':')
    if len(parts) < 2:
        return None  # Skip if the format is wrong
    company = parts[0].strip()
    ticker = parts[1].strip()
    
    # Base keywords: various forms of the company name and ticker
    base_keywords = [
        company,
        ticker,
        company.replace(" ", ""),
        company.split()[0],
        company.lower(),
        company.upper()
    ]
    
    # Add Wikidata aliases from Wikipedia for enrichment
    aliases = get_wikidata_aliases(company)
    base_keywords.extend(aliases)
    
    # Custom enhancements if desired
    enhancements = {
        "Apple Inc.": ["iPhone", "MacBook", "Tim Cook"],
        "Microsoft": ["Azure", "Xbox", "Satya Nadella"],
        "Tesla, Inc.": ["Cybertruck", "Gigafactory", "Elon Musk"]
    }
    base_keywords.extend(enhancements.get(company, []))
    
    # Remove duplicates while preserving order,
    # converting dicts to strings by extracting the 'value' if present.
    seen = set()
    unique_keywords = []
    for kw in base_keywords:
        # If kw is a dict, try to get the 'value' field, else convert to string
        if isinstance(kw, dict):
            kw_val = kw.get('value', str(kw))
        else:
            kw_val = kw
        if kw_val not in seen:
            unique_keywords.append(kw_val)
            seen.add(kw_val)
    
    # Return a colon-separated enriched string
    return f"{company}:{ticker}:" + ":".join(unique_keywords)

def main():
    input_file = "tickers.txt"       # File containing lines like "Veolia:VEOEY"
    output_file = "enriched_keywords.txt"
    enriched_lines = []
    
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                enriched = expand_keywords(line)
                if enriched:
                    enriched_lines.append(enriched)
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(enriched_lines))
    
    print(f"Enriched keywords saved to {output_file}")

if __name__ == "__main__":
    main()


