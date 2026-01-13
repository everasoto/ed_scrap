import os
import re
import time
import unicodedata
from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert

# --------------------------------------------------
# DB engine
# --------------------------------------------------
engine = create_engine(os.getenv("SUPABASE_DB_URL"))  # e.g. postgres://...

# --------------------------------------------------
# Text cleaning function
# --------------------------------------------------
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# --------------------------------------------------
# Load existing URLs from DB (for de-duplication)
# --------------------------------------------------
def load_existing_urls(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT url FROM anf_articles"))
        return {row[0] for row in result}

# --------------------------------------------------
# Article extraction
# --------------------------------------------------
def extract_article(url: str) -> dict | None:
    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Title
    title_tag = soup.select_one("h1.qtitle")
    title = clean_text(title_tag.get_text(strip=True)) if title_tag else None

    # Subheader
    subheader_tag = soup.select_one("div.qlead")
    subheader = clean_text(subheader_tag.get_text(strip=True)) if subheader_tag else None

    # Date (raw text)
    date_tag = soup.select_one("div.qdate")
    date_text = clean_text(date_tag.get_text(strip=True)) if date_tag else None

    # Main content container
    content_div = soup.select_one("div.qtexto.pt-3.anf-arti-content")
    if not content_div:
        return None

    # Extract paragraphs
    paragraphs = [
        clean_text(p.get_text(" ", strip=True))
        for p in content_div.find_all("p")
    ]
    content = " ".join(p for p in paragraphs if p)

    if not content:
        return None

    # Extract location + agency prefix (first <p>, often has city/agency)
    first_p = content_div.find("p")
    prefix = None
    if first_p:
        # Sometimes in <strong>, sometimes plain text
        strong_tag = first_p.find("strong")
        if strong_tag:
            prefix = clean_text(strong_tag.get_text(strip=True))
        else:
            prefix = clean_text(first_p.get_text(strip=True))

    # Extract author code (last <p>)
    last_p = content_div.find_all("p")[-1]
    author_code = clean_text(last_p.get_text(strip=True).strip("/ "))

    return {
        "headline": title,
        "date_extracted": date_text,
        "subheadline": subheader,
        "author": author_code,
        "content": content,
        "url": url,
        "date_agency": prefix,
    }

# --------------------------------------------------
# Section scraping
# --------------------------------------------------
base_url = "https://www.noticiasfides.com"
sections = [
    "nacional",
    "nacional/politica",
    "nacional/sociedad",
    "nacional/seguridad",
    "economia",
    "mundo",
]

list_urls = [
    f"{base_url}/{section}/?page={i}"
    for section in sections
    for i in range(1, 6)
]

articles = []
seen_urls = set()  # avoid duplicates inside this run

# Load URLs already in DB and skip them
existing_urls = load_existing_urls(engine)
print(f"Existing URLs in DB: {len(existing_urls)}")

for url in list_urls:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching list page {url}: {e}")
        continue

    soup = BeautifulSoup(response.text, "html.parser")

    # NOTE: this is a generic filter; you may want a more specific CSS selector
    for a in soup.find_all("a", href=True):
        # Skip menu / nav / obvious non-articles by class if needed
        # if a.has_attr("class"):
        #     continue

        href = a["href"]
        if not href.startswith("/"):
            continue

        # Skip pure image links with no text
        if a.find("img") and not a.get_text(strip=True):
            continue

        title = a.get_text(strip=True)
        if not title:
            continue

        full_url = base_url + href

        # Skip already processed in this run
        if full_url in seen_urls:
            continue

        # Skip URLs already stored in DB
        if full_url in existing_urls:
            continue

        seen_urls.add(full_url)

        articles.append({
            "url": full_url,
            "title": title,
        })

print(f"New article URLs to extract: {len(articles)}")

# --------------------------------------------------
# Extract articles
# --------------------------------------------------
full_articles = []

for item in articles:
    url = item["url"]
    article_data = extract_article(url)
    if article_data:
        full_articles.append(article_data)
    else:
        print(f"Failed to extract article: {url}")

if not full_articles:
    print("No new articles extracted.")
    raise SystemExit()

df = pd.DataFrame(full_articles)

# --------------------------------------------------
# Date & time normalization
# --------------------------------------------------
# Spanish → English months
months = {
    "enero": "january",
    "febrero": "february",
    "marzo": "march",
    "abril": "april",
    "mayo": "may",
    "junio": "june",
    "julio": "july",
    "agosto": "august",
    "septiembre": "september",
    "octubre": "october",
    "noviembre": "november",
    "diciembre": "december",
}

# Regex patterns for date/time inside date_extracted
date_pattern = r"(\d{1,2} de [A-Za-záéíóúñ]+ de \d{4})"
time_pattern = r"(\d{1,2}:\d{2})"

# Extract date text & time text safely
df["date_str"] = df["date_extracted"].str.extract(date_pattern, expand=False)
df["time_str"] = df["date_extracted"].str.extract(time_pattern, expand=False)

# Normalize months
df["date_str"] = df["date_str"].replace(months, regex=True)

# Parse date (day-first)
df["date"] = pd.to_datetime(
    df["date_str"],
    format="%d %B %Y",
    dayfirst=True,
    errors="coerce",
)

# Parse time
df["time"] = pd.to_datetime(
    df["time_str"],
    format="%H:%M",
    errors="coerce",
).dt.time

# --------------------------------------------------
# City & agency normalization from date_agency
# --------------------------------------------------
city_review = {
    "MADRID ": "MADRID,",
    "La Paz.": "La Paz,",
}

# Clean raw prefix
df["date_agency"] = df["date_agency"].fillna("").replace(city_review, regex=True)

# City: take text before first comma or dot
df["city"] = df["date_agency"].str.extract(r"^([^,\.]+)", expand=False)
df["city"] = df["city"].str.strip()

# Agency: try to capture content inside parentheses (ANF), fallback to last word
df["agency_raw"] = df["date_agency"].str.extract(r"\(([^)]+)\)", expand=False)
df["agency_raw"] = df["agency_raw"].fillna(
    df["date_agency"].str.split().str[-1]
)

def clean_agency(x):
    if not isinstance(x, str):
        return None
    
    # Normalize unicode dashes to a simple hyphen
    x = re.sub(r"[–—−]", "-", x)
    
    # Remove everything except letters and spaces
    x = re.sub(r"[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ ]", "", x)
    
    # Collapse multiple spaces
    x = re.sub(r"\s+", " ", x).strip()
    
    # Uppercase for consistent matching
    x_up = x.upper()
    
    # Canonical mappings
    if x_up.startswith("ANF"):
        return "ANF"
    if x_up.replace(" ", "") == "EUROPAPRESS":
        return "EUROPA PRESS"
    if x_up == "":
        return None
    
    return x  # keep rare agencies as-is

df["agency"] = df["agency_raw"].apply(clean_agency)

# --------------------------------------------------
# Section extraction from URL
# --------------------------------------------------
def extract_section(url: str) -> str | None:
    parts = url.split("/")
    # e.g. https: '' www.noticiasfides.com nacional politica algo slug
    # idx:   0     1         2               3      4       5    6
    if len(parts) < 5:
        return None
    return "/".join(parts[3:-1])

df["section"] = df["url"].apply(extract_section)

# --------------------------------------------------
# Insert into DB with ON CONFLICT DO NOTHING
# --------------------------------------------------
metadata = MetaData()
news_articles = Table("anf_articles", metadata, autoload_with=engine)

with engine.begin() as conn:
    for _, row in df.iterrows():
        data = row.to_dict()
        stmt = insert(news_articles).values(data)
        stmt = stmt.on_conflict_do_nothing(index_elements=["url"])
        conn.execute(stmt)

print("Scraping finished.")
