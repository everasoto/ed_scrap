import os
import re
import time
import random
import unicodedata
from datetime import datetime
from urllib.parse import urljoin, urlparse

import pandas as pd
import cloudscraper  # Specialized for bypassing Cloudflare
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert

# --------------------------------------------------
# Initialize Scraper & DB
# --------------------------------------------------
# cloudscraper handles the Cloudflare JavaScript challenges automatically
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

engine = create_engine(os.getenv("SUPABASE_DB_URL"))

# --------------------------------------------------
# Utility Functions
# --------------------------------------------------
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def load_existing_urls(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT url FROM anf_articles"))
        return {row[0] for row in result}

# --------------------------------------------------
# Article extraction
# --------------------------------------------------
def extract_article(url: str) -> dict | None:
    try:
        # Use scraper instead of requests
        resp = scraper.get(url, timeout=15)
        resp.raise_for_status()
        # Add human-like jitter
        time.sleep(random.uniform(1.5, 3.0))
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

    paragraphs = [
        clean_text(p.get_text(" ", strip=True))
        for p in content_div.find_all("p")
    ]
    content = " ".join(p for p in paragraphs if p)

    if not content:
        return None

    # Location/Agency prefix
    first_p = content_div.find("p")
    prefix = None
    if first_p:
        strong_tag = first_p.find("strong")
        if strong_tag:
            prefix = clean_text(strong_tag.get_text(strip=True))
        else:
            prefix = clean_text(first_p.get_text(strip=True))

    # Author code (usually last paragraph)
    all_ps = content_div.find_all("p")
    author_code = clean_text(all_ps[-1].get_text(strip=True).strip("/ ")) if all_ps else None

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
# Main Scraping Loop
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

articles_to_process = []
seen_urls = set()

# De-duplicate against DB
existing_urls = load_existing_urls(engine)
print(f"Existing URLs in DB: {len(existing_urls)}")

for url in list_urls:
    try:
        response = scraper.get(url, timeout=15)
        response.raise_for_status()
        time.sleep(random.uniform(2, 4))
    except Exception as e:
        print(f"Error fetching list page {url}: {e}")
        continue

    soup = BeautifulSoup(response.text, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("/"):
            continue
        if a.find("img") and not a.get_text(strip=True):
            continue
        
        title = a.get_text(strip=True)
        if not title:
            continue

        full_url = base_url + href

        if full_url in seen_urls or full_url in existing_urls:
            continue

        seen_urls.add(full_url)
        articles_to_process.append({"url": full_url, "title": title})

print(f"New article URLs to extract: {len(articles_to_process)}")

# --------------------------------------------------
# Data Processing & Normalization
# --------------------------------------------------
full_articles = []
for item in articles_to_process:
    data = extract_article(item["url"])
    if data:
        full_articles.append(data)

if not full_articles:
    print("No new articles extracted.")
    exit()

df = pd.DataFrame(full_articles)

# Date Normalization
months = {
    "enero": "january", "febrero": "february", "marzo": "march",
    "abril": "april", "mayo": "may", "junio": "june",
    "julio": "july", "agosto": "august", "septiembre": "september",
    "octubre": "october", "noviembre": "november", "diciembre": "december",
}

date_pattern = r"(\d{1,2} de [A-Za-záéíóúñ]+ de \d{4})"
time_pattern = r"(\d{1,2}:\d{2})"

df["date_str"] = df["date_extracted"].str.extract(date_pattern, expand=False)
df["time_str"] = df["date_extracted"].str.extract(time_pattern, expand=False)
df["date_str"] = df["date_str"].replace(months, regex=True)

df["date"] = pd.to_datetime(df["date_str"], format="%d de %B de %Y", errors="coerce")
# If the format above fails due to "de", use a cleaner replace
df["date_str_clean"] = df["date_str"].str.replace(" de ", " ")
df["date"] = pd.to_datetime(df["date_str_clean"], format="%d %B %Y", errors="coerce")

df["time"] = pd.to_datetime(df["time_str"], format="%H:%M", errors="coerce").dt.time

# City & Agency Cleanup
city_review = {"MADRID ": "MADRID,", "La Paz.": "La Paz,"}
df["date_agency"] = df["date_agency"].fillna("").replace(city_review, regex=True)
df["city"] = df["date_agency"].str.extract(r"^([^,\.]+)", expand=False).str.strip()
df["agency_raw"] = df["date_agency"].str.extract(r"\(([^)]+)\)", expand=False)
df["agency_raw"] = df["agency_raw"].fillna(df["date_agency"].str.split().str[-1])

def clean_agency(x):
    if not isinstance(x, str): return None
    x = re.sub(r"[–—−]", "-", x)
    x = re.sub(r"[^A-Za-zÁÉÍÓÚÜÑáéíóúüñ ]", "", x)
    x = re.sub(r"\s+", " ", x).strip()
    x_up = x.upper()
    if x_up.startswith("ANF"): return "ANF"
    if x_up.replace(" ", "") == "EUROPAPRESS": return "EUROPA PRESS"
    return x if x != "" else None

df["agency"] = df["agency_raw"].apply(clean_agency)

# Section extraction
def extract_section(url: str) -> str | None:
    parts = url.split("/")
    return "/".join(parts[3:-1]) if len(parts) >= 5 else None

df["section"] = df["url"].apply(extract_section)

# --------------------------------------------------
# DB Upload
# --------------------------------------------------
metadata = MetaData()
# Explicitly reflect table
news_articles = Table("anf_articles", metadata, autoload_with=engine)

# Keep only columns that exist in the DB table to avoid errors
cols_to_keep = ["headline", "subheadline", "author", "content", "url", "date", "time", "city", "agency", "section"]
final_df = df[[c for c in cols_to_keep if c in df.columns]]

with engine.begin() as conn:
    for _, row in final_df.iterrows():
        data = row.to_dict()
        stmt = insert(news_articles).values(data)
        stmt = stmt.on_conflict_do_nothing(index_elements=["url"])
        conn.execute(stmt)

print("Scraping finished successfully.")
