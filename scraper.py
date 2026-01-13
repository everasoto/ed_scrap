import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import unicodedata
from urllib.parse import urljoin, urlparse
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
import os

# -----------------------------
# Text cleaning function
# -----------------------------
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# -----------------------------
# Load existing URLs from DB
# -----------------------------
def load_existing_urls(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT url FROM news_articles"))
        return {row[0] for row in result}

# -----------------------------
# Section scraping function
# -----------------------------
def scrape_section_page(url: str, source_name: str = "", existing_urls=None):
    all_news = []
    found_new = False

    print(f"Scraping: {url}")
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"Skipping (status {response.status_code})")
            return all_news, found_new
    except Exception as e:
        print(f"Request failed: {e}")
        return all_news, found_new

    soup = BeautifulSoup(response.text, "html.parser")
    articles = soup.select("article")

    for a in articles:
        title_tag = a.find("h2") or a.find("h3") or a.find("a")
        link_tag = a.find("a")

        title = clean_text(title_tag.get_text(strip=True)) if title_tag else ""
        raw_link = link_tag["href"] if link_tag and link_tag.has_attr("href") else ""
        link = clean_text(urljoin(url, raw_link))

        if not title or not link:
            continue

        if existing_urls and link in existing_urls:
            continue  # skip already saved articles

        found_new = True
        all_news.append({
            "title": title,
            "link": link,
            "snapshot_date": datetime.now().date(),
            "source": source_name
        })

    return all_news, found_new

# -----------------------------
# Scrape all pages in sections
# -----------------------------
pages = ["pais", "economia", "santa-cruz", "opinion", "mundo", "educacion-y-sociedad"]

def scrape_initial_run(base_url, sections, source_name, num_pages=6, existing_urls=None):
    all_articles = []

    for section in sections:
        for page_num in range(num_pages):
            if page_num == 0:
                url = f"{base_url}{section}"
            else:
                url = f"{base_url}{section}/{page_num}"

            articles, found_new = scrape_section_page(url, source_name, existing_urls)
            all_articles.extend(articles)

            if not found_new:
                print(f"No new articles on {url}, stopping pagination for this section")
                break

    return all_articles

# -----------------------------
# Full article extraction
# -----------------------------
def extract_full_article(url: str) -> dict:
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return {"headline":"", "date_extracted":"", "author":"", "subheadline":"", "content":"", "url":url}

        soup = BeautifulSoup(response.text, "html.parser")

        h1 = soup.find("h1")
        headline = clean_text(h1.get_text(strip=True)) if h1 else ""

        date_tag = soup.find("div", class_="articulo__fecha")
        date = clean_text(date_tag.get_text(strip=True)) if date_tag else ""

        authors = soup.find("p", class_="autor__firmante")
        author = clean_text(authors.get_text(" ", strip=True)) if authors else ""

        subheadlines = soup.find("div", class_="articulo__intro")
        subheadline = clean_text(subheadlines.get_text(" ", strip=True)) if subheadlines else ""

        contents = soup.find("main", class_="articulo__cuerpo")
        content = clean_text(contents.get_text(" ", strip=True)) if contents else ""

        return {"headline":headline, "date_extracted":date, "author":author, "subheadline":subheadline, "content":content, "url":url}

    except Exception:
        return {"headline":"", "date_extracted":"", "author":"", "subheadline":"", "content":"", "url":url}

# -----------------------------
# Date parsing function
# -----------------------------
meses = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
}

def parse_fecha(texto):
    if not isinstance(texto, str) or not texto.strip():
        return None
    try:
        partes = texto.split()
        dia = partes[0]
        mes = meses.get(partes[2])
        anio = partes[4]
        hora = partes[-1]

        if mes is None:
            return None

        return datetime.strptime(f"{dia}-{mes}-{anio} {hora}", "%d-%m-%Y %H:%M")
    except Exception:
        return None

# -----------------------------
# Section extraction from URL
# -----------------------------
def extract_section(url):
    if not isinstance(url, str):
        return "unknown"
    parsed = urlparse(url)
    path = parsed.path.strip("/")
    if not path:
        return "unknown"
    return path.split("/")[0].lower()

# -----------------------------
# Main scraping workflow
# -----------------------------
pagina_web = "https://eldeber.com.bo/"
fuente = "El Deber"
sections = ["pais", "economia", "santa-cruz", "opinion", "mundo", "educacion-y-sociedad"]

# Database connection
db_url = os.getenv("SUPABASE_DB_URL")
engine = create_engine(db_url)

# Load existing URLs
existing_urls = load_existing_urls(engine)
print(f"Loaded {len(existing_urls)} existing URLs")

# Scrape new articles only
initial_news = scrape_initial_run(
    base_url=pagina_web,
    sections=sections,
    source_name=fuente,
    num_pages=6,
    existing_urls=existing_urls
)

df = pd.DataFrame(initial_news)
df = df[df["title"] != ""].drop_duplicates(subset=["link"])
print(f"New articles found: {len(df)}")

if len(df) > 0:
    # Extract full content
    full_data = df["link"].apply(extract_full_article)
    df_diario = pd.DataFrame(full_data.tolist())

    # Clean content
    marker = "Más noticias"
    df_diario["proper_content"] = df_diario["content"].str.rsplit(marker, n=1).str[0].str.strip()
    df_diario["suggested_news"] = df_diario["content"].str.rsplit(marker, n=1).str[1].fillna("").str.strip()

    # Date parsing
    df_diario["weekday"] = df_diario["date_extracted"].str.split(",", n=1).str[0].str.strip()
    df_diario["datetime"] = df_diario["date_extracted"].str.split(",", n=1).str[1].str.strip()
    df_diario["datetime"] = df_diario["datetime"].apply(parse_fecha)
    df_diario["date"] = df_diario["datetime"].dt.date
    df_diario["hour"] = df_diario["datetime"].dt.hour
    df_diario["snapshot_date"] = datetime.now().date()

    # Section extraction
    df_diario["section_url"] = df_diario["url"].apply(extract_section)

    # Insert into DB with ON CONFLICT DO NOTHING
    metadata = MetaData()
    news_articles = Table("ed_articles", metadata, autoload_with=engine)

    with engine.begin() as conn:
        for _, row in df_diario.iterrows():
            stmt = insert(news_articles).values(row.to_dict())
            stmt = stmt.on_conflict_do_nothing(index_elements=["url"])
            conn.execute(stmt)

print("Scraping finished.")

