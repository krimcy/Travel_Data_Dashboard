# Databricks notebook source
# ═══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD — Objective 1: How Can We Increase Premium Package Revenue?
#  Platform : Databricks Free Tier (Serverless + Unity Catalog)
#  Input    : 6 Gold managed Delta tables
# ═══════════════════════════════════════════════════════════════════════════════

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Dashboard — How Can We Increase Premium Package Revenue?

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
DATABASE_NAME = "hotel_project"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE DATABASE {DATABASE_NAME}")

COLORS = {
    "premium"  : "#2563EB", "standard" : "#94A3B8",
    "revenue"  : "#0EA5E9", "gold"     : "#F59E0B",
    "platinum" : "#8B5CF6", "silver"   : "#64748B",
    "bronze"   : "#B45309", "positive" : "#10B981",
    "bg"       : "#0F172A", "card_bg"  : "#1E293B",
    "text"     : "#F1F5F9", "subtext"  : "#94A3B8",
}
MEAL_COLORS = {"BB":"#94A3B8","SC":"#CBD5E1","HB":"#3B82F6","FB":"#1D4ED8"}

print("✅ Setup complete")

# COMMAND ----------
# MAGIC %md ### 1 · Load Gold Tables

# COMMAND ----------

df_pkg  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_revenue_by_package").toPandas()
df_conv = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_premium_conversion").toPandas()
df_loy  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_revenue_by_loyalty").toPandas()
df_seg  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_revenue_by_segment").toPandas()
df_ups  = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_upsell_opportunity").toPandas()
df_room = spark.table(f"{CATALOG_NAME}.{DATABASE_NAME}.gold_revenue_by_room").toPandas()

print("✅ All Gold tables loaded")
for name, df in [("revenue_by_package",df_pkg),("premium_conversion",df_conv),
                  ("revenue_by_loyalty",df_loy),("revenue_by_segment",df_seg),
                  ("upsell_opportunity",df_ups),("revenue_by_room",df_room)]:
    print(f"   {name:<30} {len(df):>6,} rows")

# COMMAND ----------
# MAGIC %md ### 2 · Prepare Chart Data

# COMMAND ----------

df_pkg["year_month"] = (
    df_pkg["arrival_date_year"].astype(str) + "-" +
    df_pkg["arrival_month_num"].astype(str).str.zfill(2)
)
df_pkg_agg = (df_pkg.groupby(["year_month","meal"])["total_revenue"]
              .sum().reset_index().sort_values("year_month"))

df_conv["year_month"] = (
    df_conv["arrival_date_year"].astype(str) + "-" +
    df_conv["arrival_month_num"].astype(str).str.zfill(2)
)
df_conv_agg = (
    df_conv.groupby("year_month")
    .agg(total_bookings=("total_bookings","sum"),
         premium_bookings=("premium_bookings","sum"),
         total_revenue=("total_revenue","sum"),
         premium_revenue=("premium_revenue","sum"))
    .reset_index().sort_values("year_month")
)
df_conv_agg["premium_conversion_pct"] = (
    df_conv_agg["premium_bookings"] / df_conv_agg["total_bookings"] * 100).round(1)
df_conv_agg["premium_revenue_pct"] = (
    df_conv_agg["premium_revenue"] / df_conv_agg["total_revenue"] * 100).round(1)

tier_order  = ["Platinum","Gold","Silver","Bronze"]
tier_colors = [COLORS["platinum"],COLORS["gold"],COLORS["silver"],COLORS["bronze"]]
df_loy_agg = (
    df_loy.groupby("g_loyalty_tier")
    .agg(total_bookings=("total_bookings","sum"),
         premium_bookings=("premium_bookings","sum"),
         total_revenue=("total_revenue","sum"),
         avg_adr=("avg_adr","mean"))
    .reset_index()
)
df_loy_agg["premium_rate_pct"] = (
    df_loy_agg["premium_bookings"]/df_loy_agg["total_bookings"]*100).round(1)
df_loy_agg["tier_order"] = df_loy_agg["g_loyalty_tier"].map(
    {t:i for i,t in enumerate(tier_order)})
df_loy_agg = df_loy_agg.sort_values("tier_order")

df_seg_agg = (
    df_seg[df_seg["market_segment"] != "Complementary"]
    .groupby("market_segment")
    .agg(avg_adr=("avg_adr","mean"),total_revenue=("total_revenue","sum"))
    .reset_index().sort_values("avg_adr", ascending=True)
)
df_room_agg = (
    df_room.groupby(["reserved_room_type","is_premium_room"])
    .agg(avg_adr=("avg_adr","mean"),total_revenue=("total_revenue","sum"))
    .reset_index().sort_values("avg_adr", ascending=False)
)
room_colors = [COLORS["premium"] if p else COLORS["standard"]
               for p in df_room_agg["is_premium_room"]]
df_ups_agg = (
    df_ups.groupby("g_loyalty_tier")
    .agg(standard_bookings=("standard_bookings","sum"),
         estimated_uplift=("estimated_uplift_30pct","sum"))
    .reset_index()
)
df_ups_agg["tier_order"] = df_ups_agg["g_loyalty_tier"].map(
    {t:i for i,t in enumerate(tier_order)})
df_ups_agg = df_ups_agg.sort_values("tier_order")

total_revenue    = df_conv_agg["total_revenue"].sum()
premium_revenue  = df_conv_agg["premium_revenue"].sum()
total_bookings   = df_conv_agg["total_bookings"].sum()
premium_bookings = df_conv_agg["premium_bookings"].sum()
premium_rev_pct  = premium_revenue / total_revenue * 100
premium_conv_pct = premium_bookings / total_bookings * 100
upsell_total     = df_ups_agg["standard_bookings"].sum()

print("✅ Data prepared")

# COMMAND ----------
# MAGIC %md ### 3 · KPI Cards

# COMMAND ----------

fig_kpi = go.Figure()
kpis = [
    ("Total Revenue",        f"€{total_revenue/1e6:.2f}M",   "All completed bookings",            COLORS["revenue"]),
    ("Premium Revenue",      f"€{premium_revenue/1e6:.2f}M", f"{premium_rev_pct:.1f}% of total",  COLORS["premium"]),
    ("Premium Conv. Rate",   f"{premium_conv_pct:.1f}%",     "Bookings that are HB/FB/F/G/H",     COLORS["positive"]),
    ("Upsell Opportunities", f"{upsell_total:,}",            "Loyal guests on standard packages",  COLORS["gold"]),
]
for i, (label, value, subtitle, color) in enumerate(kpis):
    x = i / 4 + 0.125
    for y, txt, sz, col in [(0.65,value,32,color),(0.30,f"<b>{label}</b>",13,COLORS["text"]),(0.08,subtitle,10,COLORS["subtext"])]:
        fig_kpi.add_annotation(x=x, y=y, text=txt,
            font=dict(size=sz, color=col, family="Arial Black" if sz==32 else "Arial"),
            showarrow=False, xref="paper", yref="paper")
fig_kpi.update_layout(
    title=dict(text="📊 How Can We Increase Premium Package Revenue?",
               font=dict(size=20, color=COLORS["text"]), x=0.5),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["bg"],
    height=180, margin=dict(l=20,r=20,t=50,b=10),
    xaxis=dict(visible=False), yaxis=dict(visible=False),
)
for x in [0.25,0.50,0.75]:
    fig_kpi.add_vline(x=x, line_color="#334155", line_width=1)
fig_kpi.show()

# COMMAND ----------
# MAGIC %md ### 4 · Chart 1 — Monthly Revenue by Meal Package

# COMMAND ----------

fig1 = go.Figure()
meal_labels = {"BB":"Bed & Breakfast","HB":"Half Board","FB":"Full Board","SC":"Self Catering"}
for meal in ["FB","HB","BB","SC"]:
    d = df_pkg_agg[df_pkg_agg["meal"]==meal]
    fig1.add_trace(go.Scatter(x=d["year_month"], y=d["total_revenue"],
        name=meal_labels.get(meal,meal), mode="lines+markers",
        line=dict(color=MEAL_COLORS.get(meal,"#888"), width=2.5), marker=dict(size=5),
        hovertemplate=f"<b>{meal_labels.get(meal,meal)}</b><br>Month: %{{x}}<br>Revenue: €%{{y:,.0f}}<extra></extra>"))
fig1.update_layout(
    title=dict(text="📈 Monthly Revenue by Meal Package", font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"], font=dict(color=COLORS["text"]),
    xaxis=dict(title="Month", gridcolor="#334155", tickangle=45, tickfont=dict(size=9)),
    yaxis=dict(title="Revenue (€)", gridcolor="#334155", tickformat="€,.0f"),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    hovermode="x unified", height=420, margin=dict(l=60,r=20,t=50,b=80),
)
fig1.add_annotation(text="💡 HB/FB guests generate 1.6–2× more revenue per booking than BB",
    xref="paper", yref="paper", x=0.01, y=-0.22,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False, align="left")
fig1.show()

# COMMAND ----------
# MAGIC %md ### 5 · Chart 2 — Premium Conversion Rate

# COMMAND ----------

fig2 = make_subplots(specs=[[{"secondary_y": True}]])
fig2.add_trace(go.Scatter(x=df_conv_agg["year_month"], y=df_conv_agg["premium_conversion_pct"],
    name="Booking Conv. Rate (%)", mode="lines+markers",
    line=dict(color=COLORS["premium"], width=2.5), marker=dict(size=6),
    hovertemplate="Month: %{x}<br>Conv: %{y:.1f}%<extra></extra>"), secondary_y=False)
fig2.add_trace(go.Scatter(x=df_conv_agg["year_month"], y=df_conv_agg["premium_revenue_pct"],
    name="Revenue Conv. Rate (%)", mode="lines+markers",
    line=dict(color=COLORS["positive"], width=2.5, dash="dot"), marker=dict(size=6),
    hovertemplate="Month: %{x}<br>Rev Conv: %{y:.1f}%<extra></extra>"), secondary_y=False)
fig2.add_trace(go.Bar(x=df_conv_agg["year_month"], y=df_conv_agg["total_bookings"],
    name="Total Bookings", marker_color="rgba(148,163,184,0.15)",
    hovertemplate="Month: %{x}<br>Bookings: %{y:,}<extra></extra>"), secondary_y=True)
fig2.update_layout(
    title=dict(text="📈 Premium Conversion Rate Over Time", font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"], font=dict(color=COLORS["text"]),
    xaxis=dict(title="Month", gridcolor="#334155", tickangle=45, tickfont=dict(size=9)),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    hovermode="x unified", height=420, margin=dict(l=60,r=60,t=50,b=80), barmode="overlay",
)
fig2.update_yaxes(title_text="Conversion Rate (%)", gridcolor="#334155", ticksuffix="%", secondary_y=False)
fig2.update_yaxes(title_text="Total Bookings", gridcolor="#334155", showgrid=False, secondary_y=True)
fig2.add_annotation(text="💡 Peak Jul–Aug (38%). Oct–Jan dips below 15% — push premium in off-peak",
    xref="paper", yref="paper", x=0.01, y=-0.22,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False, align="left")
fig2.show()

# COMMAND ----------
# MAGIC %md ### 6 · Chart 3 — Revenue by Loyalty Tier

# COMMAND ----------

fig3 = make_subplots(specs=[[{"secondary_y": True}]])
fig3.add_trace(go.Bar(x=df_loy_agg["g_loyalty_tier"], y=df_loy_agg["total_revenue"],
    name="Total Revenue (€)", marker_color=tier_colors,
    hovertemplate="<b>%{x}</b><br>Revenue: €%{y:,.0f}<extra></extra>"), secondary_y=False)
fig3.add_trace(go.Scatter(x=df_loy_agg["g_loyalty_tier"], y=df_loy_agg["premium_rate_pct"],
    name="Premium Rate (%)", mode="lines+markers",
    line=dict(color=COLORS["positive"], width=3), marker=dict(size=10, symbol="diamond"),
    hovertemplate="<b>%{x}</b><br>Premium Rate: %{y:.1f}%<extra></extra>"), secondary_y=True)
fig3.update_layout(
    title=dict(text="💎 Revenue & Premium Rate by Loyalty Tier", font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"], font=dict(color=COLORS["text"]),
    xaxis=dict(title="Loyalty Tier", gridcolor="#334155"),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    height=420, margin=dict(l=60,r=60,t=50,b=80), bargap=0.35,
)
fig3.update_yaxes(title_text="Total Revenue (€)", gridcolor="#334155", tickformat="€,.0f", secondary_y=False)
fig3.update_yaxes(title_text="Premium Rate (%)", gridcolor="#334155", ticksuffix="%", showgrid=False, secondary_y=True)
fig3.add_annotation(text="💡 Platinum: 52% premium rate vs Bronze: 12.5% — tier is the strongest predictor of premium uptake",
    xref="paper", yref="paper", x=0.01, y=-0.18,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False, align="left")
fig3.show()

# COMMAND ----------
# MAGIC %md ### 7 · Chart 4 — ADR by Market Segment

# COMMAND ----------

seg_colors = [COLORS["premium"] if a>=110 else COLORS["revenue"] if a>=80 else COLORS["standard"]
              for a in df_seg_agg["avg_adr"]]
fig4 = go.Figure(go.Bar(
    x=df_seg_agg["avg_adr"], y=df_seg_agg["market_segment"], orientation="h",
    marker_color=seg_colors,
    text=df_seg_agg["avg_adr"].apply(lambda x: f"€{x:.0f}"), textposition="outside",
    hovertemplate="<b>%{y}</b><br>Avg ADR: €%{x:.2f}<extra></extra>"
))
fig4.add_vline(x=df_seg_agg["avg_adr"].mean(), line_dash="dash",
    line_color=COLORS["gold"], line_width=1.5,
    annotation_text=f"Avg: €{df_seg_agg['avg_adr'].mean():.0f}",
    annotation_font_color=COLORS["gold"], annotation_position="top right")
fig4.update_layout(
    title=dict(text="🎯 Average Daily Rate by Market Segment", font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"], font=dict(color=COLORS["text"]),
    xaxis=dict(title="Avg Daily Rate (€)", gridcolor="#334155", tickprefix="€"),
    yaxis=dict(gridcolor="#334155"),
    height=380, margin=dict(l=130,r=60,t=50,b=60),
)
fig4.add_annotation(text="💡 Direct & Online TA have highest ADR (€114) — focus premium promotions here",
    xref="paper", yref="paper", x=0.01, y=-0.15,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False, align="left")
fig4.show()

# COMMAND ----------
# MAGIC %md ### 8 · Chart 5 — Revenue by Room Type

# COMMAND ----------

fig5 = make_subplots(specs=[[{"secondary_y": True}]])
fig5.add_trace(go.Bar(x=df_room_agg["reserved_room_type"], y=df_room_agg["total_revenue"],
    name="Total Revenue (€)", marker_color=room_colors,
    hovertemplate="Room <b>%{x}</b><br>Revenue: €%{y:,.0f}<extra></extra>"), secondary_y=False)
fig5.add_trace(go.Scatter(x=df_room_agg["reserved_room_type"], y=df_room_agg["avg_adr"],
    name="Avg ADR (€)", mode="lines+markers",
    line=dict(color=COLORS["gold"], width=3), marker=dict(size=9),
    hovertemplate="Room <b>%{x}</b><br>ADR: €%{y:.0f}<extra></extra>"), secondary_y=True)
fig5.update_layout(
    title=dict(text="🏨 Revenue & ADR by Room Type  ■ Premium (F/G/H)  ■ Standard",
               font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"], font=dict(color=COLORS["text"]),
    xaxis=dict(title="Room Type", gridcolor="#334155",
        categoryorder="array", categoryarray=df_room_agg["reserved_room_type"].tolist()),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    height=420, margin=dict(l=60,r=60,t=60,b=80), bargap=0.3,
)
fig5.update_yaxes(title_text="Total Revenue (€)", gridcolor="#334155", tickformat="€,.0f", secondary_y=False)
fig5.update_yaxes(title_text="Avg ADR (€)", gridcolor="#334155", tickprefix="€", showgrid=False, secondary_y=True)
fig5.add_annotation(text="💡 Room H: €181 ADR but only 356 bookings — premium rooms are massively underbooked",
    xref="paper", yref="paper", x=0.01, y=-0.20,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False, align="left")
fig5.show()

# COMMAND ----------
# MAGIC %md ### 9 · Chart 6 — Upsell Opportunity

# COMMAND ----------

fig6 = make_subplots(specs=[[{"secondary_y": True}]])
fig6.add_trace(go.Bar(x=df_ups_agg["g_loyalty_tier"], y=df_ups_agg["standard_bookings"],
    name="Standard Bookings (upsell targets)",
    marker_color=[COLORS["platinum"],COLORS["gold"],COLORS["silver"],COLORS["bronze"]],
    hovertemplate="<b>%{x}</b><br>Targets: %{y:,}<extra></extra>"), secondary_y=False)
fig6.add_trace(go.Scatter(x=df_ups_agg["g_loyalty_tier"], y=df_ups_agg["estimated_uplift"],
    name="Est. Uplift @ 30% conversion (€)", mode="lines+markers",
    line=dict(color=COLORS["positive"], width=3), marker=dict(size=10, symbol="diamond"),
    hovertemplate="<b>%{x}</b><br>Est. uplift: €%{y:,.0f}<extra></extra>"), secondary_y=True)
fig6.update_layout(
    title=dict(text="🚀 Upsell Opportunity — Loyal Guests on Standard Packages",
               font=dict(size=16,color=COLORS["text"])),
    paper_bgcolor=COLORS["bg"], plot_bgcolor=COLORS["card_bg"], font=dict(color=COLORS["text"]),
    xaxis=dict(title="Loyalty Tier", gridcolor="#334155"),
    legend=dict(bgcolor=COLORS["bg"], bordercolor="#334155", borderwidth=1),
    height=420, margin=dict(l=60,r=80,t=50,b=80), bargap=0.35,
)
fig6.update_yaxes(title_text="Standard Bookings", gridcolor="#334155", secondary_y=False)
fig6.update_yaxes(title_text="Est. Revenue Uplift (€)", gridcolor="#334155", tickformat="€,.0f", showgrid=False, secondary_y=True)
fig6.add_annotation(text="💡 34,851 Bronze guests on standard packages — converting 30% = major revenue uplift",
    xref="paper", yref="paper", x=0.01, y=-0.18,
    font=dict(size=11,color=COLORS["subtext"]), showarrow=False, align="left")
fig6.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📋 Key Findings & Recommendations
# MAGIC
# MAGIC | # | Finding | Recommendation |
# MAGIC |---|---------|----------------|
# MAGIC | 1 | FB guests generate **2× revenue** per booking vs BB | Push FB upgrades at checkout on Direct + Online TA |
# MAGIC | 2 | Premium conversion **peaks in summer** (38%) crashes Oct–Jan | Launch off-peak premium promotions with spa bundles |
# MAGIC | 3 | Platinum guests have **52% premium rate** vs 12.5% Bronze | Segment upsell campaigns by loyalty tier |
# MAGIC | 4 | **4,877 Gold + 660 Platinum** guests booked standard | Priority upsell: send personalised premium offers |
# MAGIC | 5 | Room H has **€181 ADR** but only 356 bookings | Increase Room H visibility in booking flow |
# MAGIC | 6 | Direct + Online TA command **€114 ADR** (highest) | Concentrate premium promotions on these channels |
# MAGIC
# MAGIC ---
# MAGIC > **Data source:** Mojtaba, "Hotel Booking Dataset," Kaggle 2020.
# MAGIC > https://www.kaggle.com/datasets/mojtaba142/hotel-booking/data
# MAGIC > Guest and package data generated synthetically from above source.
