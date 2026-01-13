import os
import re
import time
import random
import unicodedata
import cloudscraper  # <-- Added this
from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
# import requests  # We will use scraper instead
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert

# --------------------------------------------------
# Initialize Scraper Session
# --------------------------------------------------
# This creates a session that can handle Cloudflare challenges automatically
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

# --------------------------------------------------
# DB engine
# --------------------------------------------------
engine = create_engine(os.getenv("SUPABASE_DB_URL"))

# ... [clean_text and load_existing_urls functions remain the same] ...

def extract_article(url: str) -> dict | None:
    try:
        # Using scraper instead of requests
        resp = scraper.get(url, timeout=15)
        resp.raise_for_status()
        # Randomized delay to mimic human reading speed (1-3 seconds)
        time.sleep(random.uniform(1, 3))
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    # ... [rest of the extraction logic remains the same] ...

# --------------------------------------------------
# Section scraping logic
# --------------------------------------------------
# ... [sections and list_urls definitions] ...

for url in list_urls:
    try:
        # Using scraper instead of requests
        response = scraper.get(url, timeout=15)
        response.raise_for_status()
        # Small delay between list pages
        time.sleep(random.uniform(2, 4))
    except Exception as e:
        print(f"Error fetching list page {url}: {e}")
        continue

    soup = BeautifulSoup(response.text, "html.parser")
    # ... [rest of the URL gathering logic] ...
