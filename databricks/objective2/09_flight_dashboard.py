# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD — Objective 2: How Can We Improve Traveler Satisfaction
#              and Reduce Booking Friction?
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Input    : 6 Gold Delta tables (flight_project database)
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Dashboard — Traveler Satisfaction & Booking Friction
# MAGIC **Objective 2 | Medallion Architecture | Gold Layer Output**

# COMMAND ----------

%pip install plotly --quiet

# COMMAND ----------

from pyspark.sql import functions as F
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

CATALOG_NAME  = "hotel_catalog"
DATABASE_NAME = "flight_project"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

# ── Colour palette ─────────────────────────────────────────────────────────────
COLORS = {
    "primary"   : "#2563EB",
    "satisfied" : "#10B981",
    "neutral"   : "#F59E0B",
    "dissatisfied": "#EF4444",
    "noshow"    : "#8B5CF6",
    "premium"   : "#0EA5E9",
    "standard"  : "#94A3B8",
    "bg"        : "#0F172A",
    "card_bg"   : "#1E293B",
    "text"      : "#F1F5F9",
    "subtext"   : "#94A3B8",
}

TIER_COLORS = {
    "Platinum"   : "#8B5CF6",
    "Gold"       : "#F59E0B",
    "Silver"     : "#64748B",
    "Non-Member" : "#94A3B8",
}

print("✅ Setup complete")

# COMMAND ----------
# MAGIC %md ### 1 · Load Gold Tables

# COMMAND ----------

df_seg    = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_satisfaction_by_segment").toPandas()
df_route  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_satisfaction_by_route").toPandas()
df_noshow = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_noshow_by_booking_pattern").toPandas()
df_checkin= spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_friction_by_checkin").toPandas()
df_delay  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_delay_impact").toPandas()
df_profile= spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_traveler_profile").toPandas()

print("✅ All Gold tables loaded")
for name, df in [("satisfaction_by_segment",df_seg),
                  ("satisfaction_by_route",df_route),
                  ("noshow_by_booking_pattern",df_noshow),
                  ("friction_by_checkin",df_checkin),
                  ("delay_impact",df_delay),
                  ("traveler_profile",df_profile)]:
    print(f"   {name:<35} {len(df):>6,} rows")

# COMMAND ----------
# MAGIC %md ### 2 · Prepare Chart Data

# COMMAND ----------

# ── KPI headline numbers ──────────────────────────────────────────────────────
total_passengers  = df_seg["passengers"].sum()
avg_satisfaction  = (df_seg["avg_satisfaction"] * df_seg["passengers"]).sum() / total_passengers
satisfied_count   = df_seg["satisfied_count"].sum()
dissatisfied_count= df_seg["dissatisfied_count"].sum()
total_no_shows    = df_noshow["no_shows"].sum()
overall_no_show   = df_noshow["no_shows"].sum() / df_noshow["passengers"].sum()

# ── Chart 1: Satisfaction by Frequent Flyer Status ───────────────────────────
seg_tier = (
    df_seg.groupby("p_frequent_flyer_status")
    .agg(passengers=("passengers","sum"),
         avg_satisfaction=("avg_satisfaction","mean"),
         no_show_rate=("no_show_rate","mean"),
         satisfied_count=("satisfied_count","sum"),
         dissatisfied_count=("dissatisfied_count","sum"))
    .reset_index()
)
seg_tier["satisfaction_rate_pct"] = (
    seg_tier["satisfied_count"] / seg_tier["passengers"] * 100).round(1)
tier_order = ["Platinum","Gold","Silver","Non-Member"]
seg_tier["order"] = seg_tier["p_frequent_flyer_status"].map(
    {t:i for i,t in enumerate(tier_order)})
seg_tier = seg_tier.sort_values("order")

# ── Chart 2: Satisfaction Band ────────────────────────────────────────────────
band_agg = (
    df_seg.groupby("satisfaction_band" if "satisfaction_band" in df_seg.columns
                   else "p_frequent_flyer_status")
    .agg(passengers=("passengers","sum"))
    .reset_index()
) if "satisfaction_band" in df_seg.columns else None

# Derive from silver directly for accuracy
df_silver = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.silver_flights")
band_data = df_silver.groupBy("satisfaction_band").count().toPandas()
band_order= ["Highly Satisfied","Satisfied","Neutral","Dissatisfied"]
band_data["order"] = band_data["satisfaction_band"].map(
    {b:i for i,b in enumerate(band_order)})
band_data = band_data.sort_values("order")
band_colors_list = [COLORS["satisfied"],COLORS["satisfied"],
                    COLORS["neutral"],COLORS["dissatisfied"]]

# ── Chart 3: No-show by Booking Bucket ───────────────────────────────────────
noshow_bucket = (
    df_noshow.groupby("booking_bucket")
    .agg(passengers=("passengers","sum"),
         no_shows=("no_shows","sum"),
         avg_satisfaction=("avg_satisfaction","mean"))
    .reset_index()
)
noshow_bucket["no_show_rate"] = (
    noshow_bucket["no_shows"] / noshow_bucket["passengers"] * 100).round(2)
bucket_order = ["< 1 week","1-4 weeks","1-2 months","2-3 months","3+ months"]
noshow_bucket["order"] = noshow_bucket["booking_bucket"].map(
    {b:i for i,b in enumerate(bucket_order)})
noshow_bucket = noshow_bucket.sort_values("order")

# ── Chart 4: Check-in Method vs Satisfaction + No-show ───────────────────────
checkin_agg = (
    df_checkin.groupby("Check_in_Method")
    .agg(passengers=("passengers","sum"),
         no_shows=("no_shows","sum"),
         avg_satisfaction=("avg_satisfaction","mean"),
         satisfied_count=("satisfied_count","sum"))
    .reset_index()
)
checkin_agg["no_show_rate"] = (
    checkin_agg["no_shows"] / checkin_agg["passengers"] * 100).round(2)
checkin_agg["satisfaction_rate"] = (
    checkin_agg["satisfied_count"] / checkin_agg["passengers"] * 100).round(2)
checkin_agg = checkin_agg.sort_values("no_show_rate", ascending=False)

# ── Chart 5: Delay Impact ─────────────────────────────────────────────────────
delay_agg = (
    df_delay.groupby("delay_bucket")
    .agg(flights=("flights","sum"),
         avg_satisfaction=("avg_satisfaction","mean"),
         no_show_rate=("no_show_rate","mean"),
         dissatisfied_count=("dissatisfied_count","sum"))
    .reset_index()
)
delay_order = ["No Delay","< 15 min","15-30 min","30-60 min","> 60 min"]
delay_agg["order"] = delay_agg["delay_bucket"].map(
    {b:i for i,b in enumerate(delay_order)})
delay_agg = delay_agg.sort_values("order")

# ── Chart 6: Seat Class vs Satisfaction ──────────────────────────────────────
class_agg = (
    df_seg.groupby("Seat_Class")
    .agg(passengers=("passengers","sum"),
         avg_satisfaction=("avg_satisfaction","mean"),
         no_show_rate=("no_show_rate","mean"),
         avg_price_paid=("avg_price_paid","mean"))
    .reset_index()
    .sort_values("avg_satisfaction", ascending=False)
)

# ── Chart 7: Top/Bottom Routes ───────────────────────────────────────────────
top_routes    = df_route.nlargest(8, "avg_satisfaction")
bottom_routes = df_route.nsmallest(8, "avg_satisfaction")

print("✅ Chart data prepared")
print(f"   Total passengers  : {total_passengers:,}")
print(f"   Avg satisfaction  : {avg_satisfaction:.2f}/10")
print(f"   Satisfied (>=7)   : {satisfied_count:,} ({satisfied_count/total_passengers*100:.1f}%)")
print(f"   Dissatisfied (<5) : {dissatisfied_count:,}")
print(f"   Overall no-show   : {overall_no_show*100:.1f}%")

# COMMAND ----------
# MAGIC %md ### 3 · KPI Cards

# COMMAND ----------

from datetime import datetime
RUN_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

fig_kpi = go.Figure()

kpis = [
    ("Total Passengers",      f"{total_passengers:,}",
     "Flight records analysed",                          COLORS["primary"]),
    ("Avg Satisfaction",      f"{avg_satisfaction:.2f}/10",
     f"{satisfied_count/total_passengers*100:.1f}% rated ≥ 7.0",   COLORS["satisfied"]),
    ("Dissatisfaction Rate",  f"{dissatisfied_count/total_passengers*100:.1f}%",
     f"{dissatisfied_count:,} passengers rated < 5.0",  COLORS["dissatisfied"]),
    ("No-Show Rate",          f"{overall_no_show*100:.1f}%",
     f"{total_no_shows:,} passengers did not board",     COLORS["noshow"]),
]

for i,(label,value,subtitle,color) in enumerate(kpis):
    x = i/4+0.125
    for y,txt,sz,col in [
        (0.65, value,             32, color),
        (0.30, f"<b>{label}</b>", 13, COLORS["text"]),
        (0.08, subtitle,          10, COLORS["subtext"]),
    ]:
        fig_kpi.add_annotation(x=x, y=y, text=txt,
            font=dict(size=sz, color=col,
                      family="Arial Black" if sz==32 else "Arial"),
            showarrow=False, xref="paper", yref="paper")

fig_kpi.update_layout(
    title=dict(
        text=f"📊 Traveler Satisfaction & Booking Friction Dashboard — {RUN_TIMESTAMP}",
        font=dict(size=18, color=COLORS["text"]), x=0.5),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
    height=180, margin=dict(l=20,r=20,t=50,b=10),
    xaxis=dict(visible=False), yaxis=dict(visible=False))
for x in [0.25,0.50,0.75]:
    fig_kpi.add_vline(x=x, line_color="#334155", line_width=1)
fig_kpi.show()

# COMMAND ----------
# MAGIC %md ### 4 · Chart 1 — Satisfaction by Loyalty Tier

# COMMAND ----------

fig1 = make_subplots(specs=[[{"secondary_y": True}]])

tier_color_list = [TIER_COLORS.get(t, COLORS["standard"])
                   for t in seg_tier["p_frequent_flyer_status"]]

fig1.add_trace(go.Bar(
    x=seg_tier["p_frequent_flyer_status"],
    y=seg_tier["avg_satisfaction"],
    name="Avg Satisfaction Score",
    marker_color=tier_color_list,
    text=seg_tier["avg_satisfaction"].apply(lambda x: f"{x:.2f}"),
    textposition="outside",
    hovertemplate="<b>%{x}</b><br>Avg Satisfaction: %{y:.2f}<extra></extra>"
), secondary_y=False)

fig1.add_trace(go.Scatter(
    x=seg_tier["p_frequent_flyer_status"],
    y=seg_tier["no_show_rate"] * 100,
    name="No-Show Rate (%)",
    mode="lines+markers",
    line=dict(color=COLORS["noshow"], width=3),
    marker=dict(size=10, symbol="diamond"),
    hovertemplate="<b>%{x}</b><br>No-Show Rate: %{y:.1f}%<extra></extra>"
), secondary_y=True)

fig1.update_layout(
    title=dict(text="✈️ Satisfaction & No-Show Rate by Loyalty Tier",
               font=dict(size=16, color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(title="Loyalty Tier", gridcolor="#334155"),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    height=420, margin=dict(l=60,r=60,t=50,b=80), bargap=0.35)
fig1.update_yaxes(title_text="Avg Satisfaction (out of 10)",
    gridcolor="#334155", range=[6.5, 7.5], secondary_y=False)
fig1.update_yaxes(title_text="No-Show Rate (%)",
    ticksuffix="%", showgrid=False, secondary_y=True)
fig1.add_annotation(
    text="💡 Platinum members score 7.09 vs Gold 6.94 — loyalty programmes drive satisfaction",
    xref="paper", yref="paper", x=0.01, y=-0.18,
    font=dict(size=11, color=COLORS["subtext"]), showarrow=False)
fig1.show()

# COMMAND ----------
# MAGIC %md ### 5 · Chart 2 — Satisfaction Band Distribution

# COMMAND ----------

band_color_map = {
    "Highly Satisfied": COLORS["satisfied"],
    "Satisfied"       : "#34D399",
    "Neutral"         : COLORS["neutral"],
    "Dissatisfied"    : COLORS["dissatisfied"],
}
band_data["color"] = band_data["satisfaction_band"].map(band_color_map)

fig2 = go.Figure()

fig2.add_trace(go.Bar(
    x=band_data["satisfaction_band"],
    y=band_data["count"],
    marker_color=band_data["color"],
    text=band_data["count"].apply(
        lambda x: f"{x:,}<br>({x/band_data['count'].sum()*100:.1f}%)"),
    textposition="outside",
    hovertemplate="<b>%{x}</b><br>Passengers: %{y:,}<extra></extra>"
))

fig2.update_layout(
    title=dict(text="📊 Passenger Satisfaction Distribution",
               font=dict(size=16, color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(title="Satisfaction Band", gridcolor="#334155",
               categoryorder="array",
               categoryarray=["Highly Satisfied","Satisfied","Neutral","Dissatisfied"]),
    yaxis=dict(title="Number of Passengers", gridcolor="#334155"),
    height=400, margin=dict(l=60,r=20,t=50,b=80), bargap=0.3)
fig2.add_annotation(
    text="💡 9.2% passengers are Dissatisfied — targeting this group is the biggest satisfaction lever",
    xref="paper", yref="paper", x=0.01, y=-0.20,
    font=dict(size=11, color=COLORS["subtext"]), showarrow=False)
fig2.show()

# COMMAND ----------
# MAGIC %md ### 6 · Chart 3 — No-Show Rate by Booking Window

# COMMAND ----------

fig3 = make_subplots(specs=[[{"secondary_y": True}]])

bar_colors = [
    COLORS["dissatisfied"] if r > 5.5 else
    COLORS["neutral"]      if r > 5.0 else
    COLORS["satisfied"]
    for r in noshow_bucket["no_show_rate"]
]

fig3.add_trace(go.Bar(
    x=noshow_bucket["booking_bucket"],
    y=noshow_bucket["no_show_rate"],
    name="No-Show Rate (%)",
    marker_color=bar_colors,
    text=noshow_bucket["no_show_rate"].apply(lambda x: f"{x:.1f}%"),
    textposition="outside",
    hovertemplate="<b>%{x}</b><br>No-Show Rate: %{y:.1f}%<extra></extra>"
), secondary_y=False)

fig3.add_trace(go.Scatter(
    x=noshow_bucket["booking_bucket"],
    y=noshow_bucket["avg_satisfaction"],
    name="Avg Satisfaction",
    mode="lines+markers",
    line=dict(color=COLORS["primary"], width=3),
    marker=dict(size=8),
    hovertemplate="<b>%{x}</b><br>Satisfaction: %{y:.2f}<extra></extra>"
), secondary_y=True)

fig3.update_layout(
    title=dict(text="🗓️ No-Show Rate & Satisfaction by Booking Window",
               font=dict(size=16, color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(title="Booking Window", gridcolor="#334155",
               categoryorder="array", categoryarray=bucket_order),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    height=420, margin=dict(l=60,r=60,t=50,b=80), bargap=0.3)
fig3.update_yaxes(title_text="No-Show Rate (%)", gridcolor="#334155",
    ticksuffix="%", secondary_y=False)
fig3.update_yaxes(title_text="Avg Satisfaction", showgrid=False,
    range=[6.8, 7.2], secondary_y=True)
fig3.add_annotation(
    text="💡 Last-minute bookings (<1 week) have 6.2% no-show rate — highest friction point",
    xref="paper", yref="paper", x=0.01, y=-0.18,
    font=dict(size=11, color=COLORS["subtext"]), showarrow=False)
fig3.show()

# COMMAND ----------
# MAGIC %md ### 7 · Chart 4 — Check-in Method Friction Analysis

# COMMAND ----------

fig4 = make_subplots(specs=[[{"secondary_y": True}]])

checkin_colors = [
    COLORS["dissatisfied"] if r > 5.5 else
    COLORS["neutral"]      if r > 4.8 else
    COLORS["satisfied"]
    for r in checkin_agg["no_show_rate"]
]

fig4.add_trace(go.Bar(
    x=checkin_agg["Check_in_Method"],
    y=checkin_agg["no_show_rate"],
    name="No-Show Rate (%)",
    marker_color=checkin_colors,
    text=checkin_agg["no_show_rate"].apply(lambda x: f"{x:.1f}%"),
    textposition="outside",
    hovertemplate="<b>%{x}</b><br>No-Show Rate: %{y:.1f}%<extra></extra>"
), secondary_y=False)

fig4.add_trace(go.Scatter(
    x=checkin_agg["Check_in_Method"],
    y=checkin_agg["avg_satisfaction"],
    name="Avg Satisfaction",
    mode="lines+markers",
    line=dict(color=COLORS["satisfied"], width=3),
    marker=dict(size=10, symbol="diamond"),
    hovertemplate="<b>%{x}</b><br>Satisfaction: %{y:.2f}<extra></extra>"
), secondary_y=True)

fig4.update_layout(
    title=dict(text="🎫 Check-in Method: No-Show Rate & Satisfaction",
               font=dict(size=16, color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(title="Check-in Method", gridcolor="#334155"),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    height=420, margin=dict(l=60,r=60,t=50,b=80), bargap=0.35)
fig4.update_yaxes(title_text="No-Show Rate (%)", gridcolor="#334155",
    ticksuffix="%", secondary_y=False)
fig4.update_yaxes(title_text="Avg Satisfaction", showgrid=False,
    range=[6.8,7.2], secondary_y=True)
fig4.add_annotation(
    text="💡 Airport Kiosk has highest no-show (5.9%) — Mobile App has lowest satisfaction (6.94)",
    xref="paper", yref="paper", x=0.01, y=-0.18,
    font=dict(size=11, color=COLORS["subtext"]), showarrow=False)
fig4.show()

# COMMAND ----------
# MAGIC %md ### 8 · Chart 5 — Delay Impact on Satisfaction

# COMMAND ----------

delay_colors = [
    COLORS["satisfied"]    if b == "No Delay"   else
    COLORS["neutral"]      if b == "< 15 min"   else
    COLORS["neutral"]      if b == "15-30 min"  else
    COLORS["dissatisfied"] if b == "30-60 min"  else
    COLORS["dissatisfied"]
    for b in delay_agg["delay_bucket"]
]

fig5 = make_subplots(specs=[[{"secondary_y": True}]])

fig5.add_trace(go.Bar(
    x=delay_agg["delay_bucket"],
    y=delay_agg["flights"],
    name="Number of Flights",
    marker_color="rgba(148,163,184,0.2)",
    hovertemplate="<b>%{x}</b><br>Flights: %{y:,}<extra></extra>"
), secondary_y=True)

fig5.add_trace(go.Scatter(
    x=delay_agg["delay_bucket"],
    y=delay_agg["avg_satisfaction"],
    name="Avg Satisfaction",
    mode="lines+markers",
    line=dict(color=COLORS["primary"], width=3),
    marker=dict(size=12, color=delay_colors, line=dict(width=2, color="white")),
    hovertemplate="<b>%{x}</b><br>Satisfaction: %{y:.2f}<extra></extra>"
), secondary_y=False)

fig5.update_layout(
    title=dict(text="⏱️ Delay Impact on Passenger Satisfaction",
               font=dict(size=16, color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(title="Delay Bucket", gridcolor="#334155",
               categoryorder="array", categoryarray=delay_order),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    height=420, margin=dict(l=60,r=60,t=50,b=80), barmode="overlay")
fig5.update_yaxes(title_text="Avg Satisfaction Score",
    gridcolor="#334155", range=[6.8, 7.2], secondary_y=False)
fig5.update_yaxes(title_text="Number of Flights",
    showgrid=False, secondary_y=True)
fig5.add_annotation(
    text="💡 95.1% of flights have some delay — 30-60 min delays drop satisfaction below no-delay baseline",
    xref="paper", yref="paper", x=0.01, y=-0.18,
    font=dict(size=11, color=COLORS["subtext"]), showarrow=False)
fig5.show()

# COMMAND ----------
# MAGIC %md ### 9 · Chart 6 — Seat Class vs Satisfaction & Price

# COMMAND ----------

fig6 = make_subplots(specs=[[{"secondary_y": True}]])

class_colors = [
    COLORS["premium"]  if c in ["Business","First"] else COLORS["standard"]
    for c in class_agg["Seat_Class"]
]

fig6.add_trace(go.Bar(
    x=class_agg["Seat_Class"],
    y=class_agg["avg_satisfaction"],
    name="Avg Satisfaction",
    marker_color=class_colors,
    text=class_agg["avg_satisfaction"].apply(lambda x: f"{x:.2f}"),
    textposition="outside",
    hovertemplate="<b>%{x}</b><br>Satisfaction: %{y:.2f}<extra></extra>"
), secondary_y=False)

fig6.add_trace(go.Scatter(
    x=class_agg["Seat_Class"],
    y=class_agg["avg_price_paid"],
    name="Avg Price (USD)",
    mode="lines+markers",
    line=dict(color=COLORS["neutral"], width=3),
    marker=dict(size=10),
    hovertemplate="<b>%{x}</b><br>Avg Price: $%{y:.0f}<extra></extra>"
), secondary_y=True)

fig6.update_layout(
    title=dict(
        text="💺 Seat Class: Satisfaction vs Price  ■ Premium  ■ Standard",
        font=dict(size=16, color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    xaxis=dict(title="Seat Class", gridcolor="#334155"),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    height=420, margin=dict(l=60,r=60,t=60,b=80), bargap=0.35)
fig6.update_yaxes(title_text="Avg Satisfaction (out of 10)",
    gridcolor="#334155", range=[6.7, 7.2], secondary_y=False)
fig6.update_yaxes(title_text="Avg Price (USD)",
    tickprefix="$", showgrid=False, secondary_y=True)
fig6.add_annotation(
    text="💡 First Class pays most ($355) but scores lowest (6.90) — price premium not matching experience",
    xref="paper", yref="paper", x=0.01, y=-0.18,
    font=dict(size=11, color=COLORS["subtext"]), showarrow=False)
fig6.show()

# COMMAND ----------
# MAGIC %md ### 10 · Chart 7 — Top vs Bottom Routes

# COMMAND ----------

fig7 = make_subplots(rows=1, cols=2,
    subplot_titles=("🏆 Top 8 Routes (Highest Satisfaction)",
                    "⚠️ Bottom 8 Routes (Lowest Satisfaction)"))

fig7.add_trace(go.Bar(
    x=top_routes["avg_satisfaction"],
    y=top_routes["r_route_id"],
    orientation="h",
    name="Top Routes",
    marker_color=COLORS["satisfied"],
    text=top_routes["avg_satisfaction"].apply(lambda x: f"{x:.2f}"),
    textposition="outside",
    hovertemplate="<b>%{y}</b><br>Satisfaction: %{x:.2f}<extra></extra>"
), row=1, col=1)

fig7.add_trace(go.Bar(
    x=bottom_routes["avg_satisfaction"],
    y=bottom_routes["r_route_id"],
    orientation="h",
    name="Bottom Routes",
    marker_color=COLORS["dissatisfied"],
    text=bottom_routes["avg_satisfaction"].apply(lambda x: f"{x:.2f}"),
    textposition="outside",
    hovertemplate="<b>%{y}</b><br>Satisfaction: %{x:.2f}<extra></extra>"
), row=1, col=2)

fig7.update_layout(
    title=dict(text="🗺️ Route Performance — Satisfaction Scores",
               font=dict(size=16, color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"],
    font=dict(color=COLORS["text"]),
    showlegend=False,
    height=450, margin=dict(l=140,r=80,t=80,b=80))
fig7.update_xaxes(gridcolor="#334155", range=[0, 10])
fig7.update_yaxes(tickfont=dict(size=9))
fig7.add_annotation(
    text="💡 Improve bottom routes by addressing delays, weather resilience, and check-in experience",
    xref="paper", yref="paper", x=0.01, y=-0.12,
    font=dict(size=11, color=COLORS["subtext"]), showarrow=False)
fig7.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📋 Key Findings & Recommendations
# MAGIC
# MAGIC | # | Finding | Recommendation |
# MAGIC |---|---------|----------------|
# MAGIC | 1 | **9.2% passengers are Dissatisfied** (score < 5) | Investigate and fix root causes for this group first |
# MAGIC | 2 | **Platinum members score 7.09** vs Gold 6.94 | Expand loyalty programme benefits to Gold tier |
# MAGIC | 3 | **Last-minute bookings have 6.2% no-show rate** | Send reminders + flexible rebooking for <1 week bookings |
# MAGIC | 4 | **Airport Kiosk has highest no-show (5.9%)** | Improve Kiosk confirmation flow + send SMS alerts |
# MAGIC | 5 | **Mobile App has lowest satisfaction (6.94)** | Prioritise Mobile App UX improvements |
# MAGIC | 6 | **First Class pays most but scores lowest (6.90)** | Audit First Class service quality — expectation gap |
# MAGIC | 7 | **95.1% flights have some delay** | Proactive delay communication improves perceived satisfaction |
# MAGIC
# MAGIC ---
# MAGIC > **Data source:** Keatonballard, "Synthetic Airline Passenger and Flight Data," Kaggle, 2024.
# MAGIC > https://www.kaggle.com/datasets/keatonballard/synthetic-airline-passenger-and-flight-data
# MAGIC > Passenger profile and route data generated synthetically from above source.
