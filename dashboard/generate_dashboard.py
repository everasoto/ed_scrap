import pandas as pd
import plotly.express as px
import plotly.colors as pc
from sqlalchemy import create_engine
from jinja2 import Template
import os

# Load DB URL
db_url = os.getenv("SUPABASE_DB_URL")
engine = create_engine(db_url)

# Load data
df = pd.read_sql("SELECT * FROM ed_articles", engine)

# Convert dates
df["date"] = pd.to_datetime(df["datetime"])
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

# Automatically create a color map for sections
sections = sorted(df["section_url"].unique())
palette = pc.qualitative.Dark24
color_map = {section: palette[i % len(palette)] for i, section in enumerate(sections)}

df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
df["month_label"] = df["date"].dt.strftime("%b %Y").str.capitalize()

section_history = (
    df.groupby(["month", "month_label", "section_url"])
      .size()
      .reset_index(name="count")
)

fig_section_history = px.bar(
    section_history,
    x="month_label",
    y="count",
    color="section_url",
    title="Histórico de artículos por sección",
    barmode="stack",
    color_discrete_map=color_map
)
fig_section_history.update_layout(legend_title_text=None)
fig_section_history.update_xaxes(categoryorder="array",
                                 categoryarray=section_history["month"].sort_values().unique())

section_history_json = fig_section_history.to_json()

# -----------------------------
# 3. Daily article count
# -----------------------------
evolution = df.groupby(["section_url", "date"]).size().reset_index(name="counts")

fig_daily = px.bar(evolution, x="date", y="counts", color="section_url", title="Number of Articles by Section Over Time",
                   #color_discrete_map=color_map, 
                   #barmode="stack"
                  )
fig_daily.update_layout(legend_title_text=None)
fig_daily.update_xaxes(categoryorder="array",
                 categoryarray=section_history["month"].sort_values().unique())

daily_counts_json = fig_daily.to_json()

# -----------------------------
# 4. Pie chart: section distribution
# -----------------------------
distribution = df['section_url'].value_counts().reset_index()
distribution.columns = ['section_url', 'counts']
distribution = distribution.sort_values(by='section_url')

# Bar chart of sections
fig_bar = px.bar(
    distribution,
    x="section_url",
    y="counts",
    color="section_url",
    title="Distribution of Articles by Section",
    color_discrete_map=color_map
)

# Optional: sort bars in descending order
fig_bar.update_layout(xaxis={'categoryorder':'total descending'})

section_bar_json = fig_bar.to_json()

# -----------------------------
# 5. Render HTML dashboard
# -----------------------------
with open("dashboard/templates/index_template.html", "r", encoding="utf-8") as f:
    template = Template(f.read())

html_output = template.render(
    latest_summary=latest_summary_html,
    section_history_json=section_history_json,
    daily_counts_json=daily_counts_json,
    section_pie_json=section_bar_json
)

with open("docs/index.html", "w", encoding="utf-8") as f:
    f.write(html_output)

print("Dashboard interactivo generado correctamente.")
