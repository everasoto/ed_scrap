import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import os
from jinja2 import Template

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

plt.figure(figsize=(14, 6))
sns.lineplot(data=section_history, x="month", y="count", hue="section_url")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("docs/section_history.png")
plt.close()

# -----------------------------
# 3. Daily article count
# -----------------------------
df["date"] = df["datetime"].dt.date

daily_counts = df.groupby("date").size().reset_index(name="count")

plt.figure(figsize=(14, 6))
sns.lineplot(data=daily_counts, x="date", y="count")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("docs/daily_counts.png")
plt.close()

# -----------------------------
# 4. Render HTML dashboard
# -----------------------------
with open("dashboard/templates/index_template.html", "r", encoding="utf-8") as f:
    template = Template(f.read())

html_output = template.render(
    latest_summary=latest_summary_html
)

with open("docs/index.html", "w", encoding="utf-8") as f:
    f.write(html_output)

print("Dashboard generado correctamente.")
