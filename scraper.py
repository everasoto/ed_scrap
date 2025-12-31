import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import unicodedata
from urllib.parse import urljoin
from datetime import datetime
from urllib.parse import urlparse

# Text cleaning function
def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

# Section scrapping function
def scrape_section_page(url: str, source_name: str = ""):
    all_news = []

    print(f"Scraping: {url}")
    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        print(f"Skipping (status {response.status_code})")
        return all_news

    soup = BeautifulSoup(response.text, "html.parser")
    articles = soup.select("article")

    for a in articles:
        title_tag = a.find("h2") or a.find("h3") or a.find("a")
        link_tag = a.find("a")

        title = clean_text(title_tag.get_text(strip=True)) if title_tag else ""
        raw_link = link_tag["href"] if link_tag and link_tag.has_attr("href") else ""
        link = clean_text(urljoin(url, raw_link))

        all_news.append({
            "title": title,
            "link": link,
            "snapshot_date": datetime.now().date(),
            "source": source_name
        })

    return all_news

# Reduced function for retrieve the links of the selected sections
pages = ["pais", "economia", "santa-cruz", "opinion", "mundo", "educacion-y-sociedad"] # Actualizar

def scrape_news_site(base_url: str, source_name: str = ""):
    all_news = []

    for page in pages:
        url = f"{base_url}{page}"
        all_news.extend(scrape_section_page(url, source_name))

    return all_news

# Function for the initial scrapping
def scrape_initial_run(base_url, sections, source_name, num_pages=6):
    all_articles = []

    for section in sections:
        for page in range(num_pages):
            if page == 0:
                url = f"{base_url}{section}"
            else:
                url = f"{base_url}{section}/{page}"

            articles = scrape_section_page(url, source_name)
            all_articles.extend(articles)

    return all_articles

# Article content extraction
def extract_full_article(url: str) -> dict:
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return {}

        soup = BeautifulSoup(response.text, "html.parser")

        # -------------------------
        # HEADLINE
        # -------------------------
        h1 = soup.find("h1")
        headline = clean_text(h1.get_text(strip=True)) if h1 else ""

        # -------------------------
        # DATE (clean container)
        # -------------------------
        date_tag = soup.find("div", class_="articulo__fecha")
        date = clean_text(date_tag.get_text(strip=True)) if date_tag else ""

        # -------------------------
        # AUTHOR (inside first block)
        # -------------------------
        authors = soup.find("p", class_="autor__firmante")
        author = clean_text(authors.get_text(" ", strip=True)) if authors else ""

        # -------------------------
        # SUBHEADLINE
        # -------------------------
        subheadlines = soup.find("div", class_="articulo__intro")
        subheadline = clean_text(subheadlines.get_text(" ", strip=True)) if subheadlines else ""

        # -------------------------
        # CONTENT
        # -------------------------
        contents = soup.find("main", class_="articulo__cuerpo")
        content = clean_text(contents.get_text(" ", strip=True)) if contents else ""

        return {
            "headline": headline,
            "date_extracted": date,
            "author": author,
            "subheadline": subheadline,
            "content": content,
            "url": url
        }

    except Exception:
        return {}
    
# Date conversion function
meses = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
}

def parse_fecha(texto):
    # 1. Si es NaN, None o no es string → devolver None o pd.NaT
    if not isinstance(texto, str):
        return None  # o pd.NaT si prefieres

    texto = texto.strip()
    if not texto:
        return None

    # 2. Intentar parsear normalmente
    try:
        partes = texto.split()
        dia = partes[0]
        mes = meses.get(partes[2], None)
        anio = partes[4]
        hora = partes[-1]

        if mes is None:
            return None

        return datetime.strptime(f"{dia}-{mes}-{anio} {hora}", "%d-%m-%Y %H:%M")

    except Exception:
        # 3. Si algo raro pasa, no romper el scraper
        return None

# Section extraction from url
def extract_section(url):
    parsed = urlparse(url)
    path = parsed.path.strip("/")  # e.g. "santa-cruz/basura-acumulada..."
    if not path:
        return "unknown"
    
    first_segment = path.split("/")[0]  # e.g. "santa-cruz"
    return first_segment.lower()

# Initial scrapping
pagina_web = "https://eldeber.com.bo/"
fuente = "El Deber"

sections = ["pais", "economia", "santa-cruz", "opinion", "mundo", "educacion-y-sociedad"]

# 1. Scrape all sections and all 6 pages
initial_news = scrape_initial_run(
    base_url=pagina_web,
    sections=sections,
    source_name=fuente,
    num_pages=6
)

# 2. Convert to DataFrame
df = pd.DataFrame(initial_news)

# 3. Clean duplicates
df = df[df["title"] != ""]
df = df.drop_duplicates(subset=["title"])

# 4. Extract full article content
full_data = df["link"].apply(extract_full_article)
df_diario = pd.DataFrame(full_data.tolist())

# "content" column split
marker = "Más noticias"

df_diario["proper_content"] = df_diario["content"].str.rsplit(marker, n=1).str[0].str.strip()
df_diario["suggested_news"] = df_diario["content"].str.rsplit(marker, n=1).str[1].fillna("").str.strip()

# Date splitting
df_diario["weekday"] = df_diario["date_extracted"].str.split(",", n=1).str[0].str.strip()

df_diario["datetime"] = df_diario["date_extracted"].str.split(",", n=1).str[1].str.strip()
df_diario["datetime"] = df_diario["datetime"].apply(parse_fecha)
df_diario["date"] = df_diario["datetime"].dt.date
df_diario["hour"] = df_diario["datetime"].dt.hour
df_diario["snapshot_date"] = datetime.now().date()

# Section extraction
df_diario["section_url"] = df_diario["url"].apply(extract_section)

from sqlalchemy import create_engine
import os

# Load the connection string from GitHub Actions
db_url = os.getenv("SUPABASE_DB_URL")

# Create the engine
engine = create_engine(db_url)

# Insert your DataFrame into the table
df_diario.to_sql(
    "news_articles",
    engine,
    if_exists="append",
    index=False
)


