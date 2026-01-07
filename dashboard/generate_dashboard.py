import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from jinja2 import Template
import os

# Load DB URL
db_url = os.getenv("SUPABASE_DB_URL")
engine = create_engine(db_url)

# Load data
df = pd.read_sql("SELECT * FROM news_articles", engine)

# Convert dates
df["datetime"] = pd.to_datetime(df["datetime"])
df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])

# -----------------------------
# 1. Latest run summary
# -----------------------------
latest_date = df["snapshot_date"].max()
latest = df[df["snapshot_date"] == latest_date]

latest_summary_html = f"""
<table>
<tr><th>Métrica</th><th>Valor</th></tr>
<tr><td>Fecha del último run</td><td>{latest_date.date()}</td></tr>
<tr><td>Artículos nuevos</td><td>{len(latest)}</td></tr>
<tr><td>Total artículos en BD</td><td>{len(df)}</td></tr>
<tr><td>Secciones actualizadas</td><td>{latest['section_url'].nunique()}</td></tr>
</table>
"""

# -----------------------------
# 2. Historical: articles per section
# -----------------------------
df["month"] = df["datetime"].dt.to_period("M").astype(str)

section_history = (
    df.groupby(["month", "section_url"])
      .size()
      .reset_index(name="count")
)

fig_section_history = px.line(
    section_history,
    x="month",
    y="count",
    color="section_url",
    title="Histórico de artículos por sección"
)

section_history_json = fig_section_history.to_json()

# -----------------------------
# 3. Daily article count
# -----------------------------
df["date"] = df["datetime"].dt.date

daily_counts = df.groupby("date").size().reset_index(name="count")

fig_daily = px.line(
    daily_counts,
    x="date",
    y="count",
    title="Artículos por día"
)

daily_counts_json = fig_daily.to_json()

# -----------------------------
# 4. Pie chart: section distribution
# -----------------------------
section_dist = df["section_url"].value_counts().reset_index()
section_dist.columns = ["section_url", "count"]

fig_pie = px.pie(
    section_dist,
    names="section_url",
    values="count",
    title="Distribución total por sección",
    hole=0.3
)

section_pie_json = fig_pie.to_json()

# -----------------------------
# 5. Render HTML dashboard
# -----------------------------
with open("dashboard/templates/index_template.html", "r", encoding="utf-8") as f:
    template = Template(f.read())

html_output = template.render(
    latest_summary=latest_summary_html,
    section_history_json=section_history_json,
    daily_counts_json=daily_counts_json,
    section_pie_json=section_pie_json
)

with open("docs/index.html", "w", encoding="utf-8") as f:
    f.write(html_output)

print("Dashboard interactivo generado correctamente.")
