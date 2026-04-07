import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats
import numpy as np
import anthropic
import json

DB_PATH = "data/warehouse/ads_intelligence.duckdb"

st.set_page_config(
    page_title="Google Ads Campaign Health Intelligence",
    page_icon="📊",
    layout="wide"
)

# ─────────────────────────────────────────
# LLM Advisor — Claude-powered campaign recommendations
# ─────────────────────────────────────────
def get_llm_recommendation(campaign_context: dict) -> str:
    client = anthropic.Anthropic()
    prompt = f"""You are a Senior Google Ads Performance Analyst at a top-tier media agency.
A campaign manager needs your expert read on a campaign that is underperforming.

Here is the campaign data:
{json.dumps(campaign_context, indent=2)}

Using Google Ads best practices, provide:
1. A one-sentence diagnosis of the core problem.
2. Two specific, actionable recommendations using real Google Ads terminology
   (e.g. Smart Bidding, tCPA, tROAS, Performance Max, bid adjustments, audience exclusions).
3. A risk flag if the anomaly rate suggests auction instability.

Be direct, specific, and concise. Write for a campaign manager who knows Google Ads well.
Do not use generic advice. Reference the actual numbers in the data."""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text


@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data
def load_table(query):
    con = get_connection()
    return con.execute(query).df()


# ─────────────────────────────────────────
# Load data
# ─────────────────────────────────────────
health_df = load_table("SELECT * FROM mart_campaign_health")
fact_df   = load_table("SELECT * FROM fact_campaign_daily")
campaigns_df = load_table("SELECT * FROM dim_campaigns")
advertisers_df = load_table("SELECT * FROM dim_advertisers")
fact_df["date"] = pd.to_datetime(fact_df["date"])


# ─────────────────────────────────────────
# Sidebar — personalization layer
# ─────────────────────────────────────────
st.sidebar.title("Your Dashboard")

# Optimization goal drives which KPIs are prominent
goal_options = ["All Goals", "brand_awareness", "lead_generation", "direct_sales"]
selected_goal = st.sidebar.selectbox("Optimization Goal", goal_options)

industries = ["All"] + sorted(health_df["industry"].dropna().unique().tolist())
selected_industry = st.sidebar.selectbox("Industry", industries)

rag_options = ["All", "GREEN", "AMBER", "RED"]
selected_rag = st.sidebar.selectbox("Health Status", rag_options)

# Primary KPI label adapts to goal
kpi_label_map = {
    "brand_awareness": "impressions",
    "lead_generation": "cvr",
    "direct_sales": "roas",
    "All Goals": "roas"
}
primary_kpi = kpi_label_map[selected_goal]

# Apply filters
filtered = health_df.copy()
if selected_goal != "All Goals":
    filtered = filtered[filtered["optimization_goal"] == selected_goal]
if selected_industry != "All":
    filtered = filtered[filtered["industry"] == selected_industry]
if selected_rag != "All":
    filtered = filtered[filtered["rag_status"] == selected_rag]

fact_filtered = fact_df.copy()
if selected_goal != "All Goals":
    fact_filtered = fact_filtered[fact_filtered["optimization_goal"] == selected_goal]


# ─────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📋 Health Scorecard",
    "🚨 Anomaly Detection",
    "🧪 A/B Test Analyzer",
    "🤖 LLM Campaign Advisor",
    "📰 Executive Summary"
])


# ══════════════════════════════════════════
# TAB 1 — Health Scorecard (goal-personalized)
# ══════════════════════════════════════════
with tab1:
    st.header("Campaign Health Scorecard")

    # KPI cards adapt to optimization goal
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Campaigns", len(filtered))
    col2.metric("GREEN", len(filtered[filtered["rag_status"] == "GREEN"]))
    col3.metric("AMBER",  len(filtered[filtered["rag_status"] == "AMBER"]))
    col4.metric("RED",    len(filtered[filtered["rag_status"] == "RED"]))

    if primary_kpi == "roas":
        col5.metric("Avg ROAS", f"{filtered['avg_roas'].mean():.2f}x")
    elif primary_kpi == "cvr":
        col5.metric("Avg CVR", f"{filtered['avg_cvr'].mean():.2%}")
    else:
        col5.metric("Avg CTR", f"{filtered['avg_ctr'].mean():.2%}")

    st.caption(f"Showing primary KPI: **{primary_kpi}** — aligned to your optimization goal: *{selected_goal}*")
    st.divider()

    def rag_color(val):
        colors = {"GREEN": "background-color: #d4edda", "AMBER": "background-color: #fff3cd", "RED": "background-color: #f8d7da"}
        return colors.get(val, "")

    display_cols = ["campaign_name", "advertiser_name", "industry", "campaign_type",
                    "bid_strategy", "avg_ctr", "avg_cpc", "avg_cvr", "avg_roas",
                    "total_spend_usd", "total_conversions", "anomaly_rate", "health_score", "rag_status"]

    styled = (filtered[display_cols]
              .sort_values("health_score", ascending=False)
              .style
              .applymap(rag_color, subset=["rag_status"])
              .format({
                  "avg_ctr": "{:.2%}",
                  "avg_cpc": "${:.2f}",
                  "avg_cvr": "{:.2%}",
                  "avg_roas": "{:.2f}x",
                  "total_spend_usd": "${:,.0f}",
                  "anomaly_rate": "{:.1%}",
                  "health_score": "{:.1f}"
              }))
    st.dataframe(styled, use_container_width=True, height=420)

    st.subheader("Health Score by Campaign Type")
    fig = px.box(filtered, x="campaign_type", y="health_score", color="rag_status",
                 color_discrete_map={"GREEN": "#28a745", "AMBER": "#ffc107", "RED": "#dc3545"})
    fig.update_layout(height=360, plot_bgcolor="white", paper_bgcolor="white")
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════
# TAB 2 — Anomaly Detection
# ══════════════════════════════════════════
with tab2:
    st.header("CPC & ROAS Anomaly Detection")

    campaigns_list = fact_filtered["campaign_id"].unique().tolist()
    selected_camp = st.selectbox("Select Campaign", sorted(campaigns_list))

    camp_data = fact_df[fact_df["campaign_id"] == selected_camp].sort_values("date")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("CPC vs 7-Day Rolling Avg")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=camp_data["date"], y=camp_data["cpc"],
                                 mode="lines", name="Daily CPC", line=dict(color="#1f77b4")))
        fig.add_trace(go.Scatter(x=camp_data["date"], y=camp_data["rolling_7d_cpc"],
                                 mode="lines", name="7D Avg", line=dict(color="#ff7f0e", dash="dash")))
        anomalies = camp_data[camp_data["cpc_anomaly_flag"] == True]
        fig.add_trace(go.Scatter(x=anomalies["date"], y=anomalies["cpc"],
                                 mode="markers", name="Anomaly",
                                 marker=dict(color="red", size=9, symbol="x")))
        fig.update_layout(height=320, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("ROAS vs 7-Day Rolling Avg")
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=camp_data["date"], y=camp_data["roas"],
                                  mode="lines", name="Daily ROAS", line=dict(color="#2ca02c")))
        fig2.add_trace(go.Scatter(x=camp_data["date"], y=camp_data["rolling_7d_roas"],
                                  mode="lines", name="7D Avg", line=dict(color="#d62728", dash="dash")))
        roas_drops = camp_data[camp_data["roas_anomaly_flag"] == True]
        fig2.add_trace(go.Scatter(x=roas_drops["date"], y=roas_drops["roas"],
                                  mode="markers", name="ROAS Drop",
                                  marker=dict(color="red", size=9, symbol="x")))
        fig2.update_layout(height=320, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Anomaly Rate by Bid Strategy")
    anomaly_by_bid = (
        fact_df.merge(campaigns_df[["campaign_id", "bid_strategy"]], on="campaign_id")
        .groupby("bid_strategy")
        .agg(anomaly_days=("cpc_anomaly_flag", "sum"), total_days=("cpc_anomaly_flag", "count"))
        .reset_index()
    )
    anomaly_by_bid["anomaly_rate_pct"] = anomaly_by_bid["anomaly_days"] / anomaly_by_bid["total_days"] * 100
    fig3 = px.bar(anomaly_by_bid, x="bid_strategy", y="anomaly_rate_pct",
                  color="anomaly_rate_pct", color_continuous_scale="RdYlGn_r")
    fig3.update_layout(height=300, plot_bgcolor="white", paper_bgcolor="white")
    st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════
# TAB 3 — A/B Test Analyzer (goal-aware)
# ══════════════════════════════════════════
with tab3:
    st.header("A/B Creative Variant Test Analyzer")
    st.caption(f"Results filtered to optimization goal: **{selected_goal}**")

    fact_with_variant = fact_filtered.merge(
        campaigns_df[["campaign_id", "creative_variant"]], on="campaign_id")

    variant_a = fact_with_variant[fact_with_variant["creative_variant"] == "A"]
    variant_b = fact_with_variant[fact_with_variant["creative_variant"] == "B"]

    # Default metric to primary KPI for current goal
    metric_options = ["ctr", "cvr", "roas", "cpc"]
    default_metric = primary_kpi if primary_kpi in metric_options else "cvr"
    metric = st.radio("Metric", metric_options,
                      index=metric_options.index(default_metric), horizontal=True)

    a_vals = variant_a[metric].dropna()
    b_vals = variant_b[metric].dropna()

    stat, p_value = stats.mannwhitneyu(a_vals, b_vals, alternative="two-sided")
    lift = (b_vals.mean() - a_vals.mean()) / a_vals.mean() * 100
    winner = "B" if b_vals.mean() > a_vals.mean() else "A"
    significant = p_value < 0.05

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Variant A Mean", f"{a_vals.mean():.4f}")
    col2.metric("Variant B Mean", f"{b_vals.mean():.4f}")
    col3.metric("Lift (B vs A)", f"{lift:+.2f}%")
    col4.metric("P-Value", f"{p_value:.4f}")

    st.divider()
    if significant:
        st.success(
            f"**Variant {winner}** wins on `{metric}` (p={p_value:.4f}, lift={lift:+.2f}%). "
            f"Recommend allocating 100% of budget to Variant {winner} and pausing the underperformer."
        )
    else:
        st.warning(
            f"No statistically significant difference on `{metric}` (p={p_value:.4f}). "
            f"Continue running — typically need 2–4 weeks of data for conclusive results in Smart Bidding campaigns."
        )

    col_a, col_b = st.columns(2)
    with col_a:
        fig_a = px.histogram(a_vals, nbins=40, title="Variant A",
                             color_discrete_sequence=["#1f77b4"])
        fig_a.update_layout(height=280, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig_a, use_container_width=True)
    with col_b:
        fig_b = px.histogram(b_vals, nbins=40, title="Variant B",
                             color_discrete_sequence=["#ff7f0e"])
        fig_b.update_layout(height=280, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig_b, use_container_width=True)


# ══════════════════════════════════════════
# TAB 4 — LLM Campaign Advisor
# ══════════════════════════════════════════
with tab4:
    st.header("LLM Campaign Advisor")
    st.caption("Powered by Claude — expert Google Ads recommendations from your live campaign data.")

    red_campaigns = health_df[health_df["rag_status"] == "RED"].copy()

    if red_campaigns.empty:
        st.success("No RED campaigns right now. All campaigns are healthy.")
    else:
        campaign_options = red_campaigns["campaign_id"].tolist()
        selected_for_advice = st.selectbox(
            "Select a RED campaign to diagnose",
            campaign_options,
            format_func=lambda x: f"{x} — {red_campaigns[red_campaigns['campaign_id']==x]['campaign_name'].values[0]}"
        )

        camp_row = red_campaigns[red_campaigns["campaign_id"] == selected_for_advice].iloc[0]

        context = {
            "campaign_id": camp_row["campaign_id"],
            "campaign_name": camp_row["campaign_name"],
            "campaign_type": camp_row["campaign_type"],
            "bid_strategy": camp_row["bid_strategy"],
            "optimization_goal": camp_row["optimization_goal"],
            "industry": camp_row["industry"],
            "avg_ctr": round(float(camp_row["avg_ctr"]), 4),
            "avg_cpc": round(float(camp_row["avg_cpc"]), 2),
            "avg_cvr": round(float(camp_row["avg_cvr"]), 4),
            "avg_roas": round(float(camp_row["avg_roas"]), 2),
            "total_spend_usd": round(float(camp_row["total_spend_usd"]), 2),
            "anomaly_rate": round(float(camp_row["anomaly_rate"]), 3),
            "anomaly_days": int(camp_row["anomaly_days"]),
            "total_days": int(camp_row["total_days"]),
            "health_score": round(float(camp_row["health_score"]), 1),
            "rag_status": camp_row["rag_status"]
        }

        with st.expander("Campaign context sent to advisor"):
            st.json(context)

        if st.button("Get Recommendation ↗"):
            with st.spinner("Analyzing campaign data..."):
                try:
                    recommendation = get_llm_recommendation(context)
                    st.subheader("Advisor Recommendation")
                    st.markdown(recommendation)
                except Exception as e:
                    st.error(f"Advisor unavailable: {e}")

    st.divider()
    st.subheader("Ask the Advisor Anything")
    user_question = st.text_area(
        "Ask a question about your campaigns",
        placeholder="e.g. Why is my target_roas strategy underperforming in mobile? What should I do?"
    )
    if st.button("Ask ↗") and user_question:
        snapshot = {
            "total_campaigns": len(health_df),
            "red_campaigns": len(health_df[health_df["rag_status"] == "RED"]),
            "avg_roas_all": round(health_df["avg_roas"].mean(), 2),
            "avg_ctr_all": round(health_df["avg_ctr"].mean(), 4),
            "avg_cvr_all": round(health_df["avg_cvr"].mean(), 4),
            "top_industry_by_spend": health_df.groupby("industry")["total_spend_usd"].sum().idxmax(),
            "selected_goal_filter": selected_goal
        }
        client = anthropic.Anthropic()
        with st.spinner("Thinking..."):
            try:
                response = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=500,
                    messages=[{
                        "role": "user",
                        "content": f"""You are a Senior Google Ads BI Analyst.
Portfolio snapshot: {json.dumps(snapshot, indent=2)}
Question: {user_question}
Answer specifically using Google Ads terminology and the data above."""
                    }]
                )
                st.markdown(response.content[0].text)
            except Exception as e:
                st.error(f"Error: {e}")


# ══════════════════════════════════════════
# TAB 5 — Executive Summary (emotional voice)
# ══════════════════════════════════════════
with tab5:
    st.header("Executive Summary")

    last_7  = fact_df[fact_df["date"] >= fact_df["date"].max() - pd.Timedelta(days=6)]
    prev_7  = fact_df[(fact_df["date"] < fact_df["date"].max() - pd.Timedelta(days=6)) &
                      (fact_df["date"] >= fact_df["date"].max() - pd.Timedelta(days=13))]

    total_spend   = last_7["spend_usd"].sum()
    total_revenue = last_7["revenue_usd"].sum()
    prev_spend    = prev_7["spend_usd"].sum()
    spend_delta   = (total_spend - prev_spend) / prev_spend * 100 if prev_spend > 0 else 0
    overall_roas  = total_revenue / total_spend if total_spend > 0 else 0
    total_conv    = last_7["conversions"].sum()
    cpc_anomalies = int(last_7["cpc_anomaly_flag"].sum())
    roas_anomalies= int(last_7["roas_anomaly_flag"].sum())
    red_count     = len(health_df[health_df["rag_status"] == "RED"])
    green_count   = len(health_df[health_df["rag_status"] == "GREEN"])

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Spend (7d)",        f"${total_spend:,.0f}",  delta=f"{spend_delta:+.1f}% WoW")
    col2.metric("Revenue (7d)",      f"${total_revenue:,.0f}")
    col3.metric("Overall ROAS",      f"{overall_roas:.2f}x")
    col4.metric("Conversions (7d)",  f"{total_conv:,}")
    col5.metric("RED Campaigns",     red_count, delta=f"{green_count} healthy", delta_color="inverse")

    st.divider()

    # Emotional + functional brief
    underperforming = health_df[health_df["rag_status"] == "RED"]["campaign_name"].head(3).tolist()
    top_industry    = health_df.groupby("industry")["total_spend_usd"].sum().idxmax()
    fwv = fact_df.merge(campaigns_df[["campaign_id", "creative_variant"]], on="campaign_id")
    variant_means   = fwv.groupby("creative_variant")["roas"].mean()
    best_variant    = variant_means.idxmax() if not variant_means.empty else "A"

    if red_count > 5:
        urgency = f"⚠️ **{red_count} campaigns are actively wasting budget right now.** This is not a monitoring problem — it's a revenue problem."
    elif red_count > 0:
        urgency = f"📌 **{red_count} campaign(s) need attention before they compound into a larger spend efficiency issue.**"
    else:
        urgency = "✅ **All campaigns are within healthy performance thresholds.** Good week."

    brief = f"""
{urgency}

**What the numbers say:**
Total ad spend reached **${total_spend:,.0f}** over the past 7 days ({spend_delta:+.1f}% week-over-week),
generating **${total_revenue:,.0f}** in revenue at an overall ROAS of **{overall_roas:.2f}x**.
{cpc_anomalies} CPC spikes and {roas_anomalies} ROAS drops were detected — signs of auction volatility
that Smart Bidding has not yet corrected.

**What needs to happen today:**
{f"Campaigns requiring immediate review: {', '.join(underperforming)}." if underperforming else "No campaigns in critical state."}
The **{top_industry}** vertical is your highest-spend segment — any instability there has outsized impact on portfolio ROAS.
Creative Variant **{best_variant}** is delivering higher ROAS and should be prioritised for budget allocation.

**The bigger picture:**
A 1-point improvement in overall CVR at this spend level translates to roughly
**{total_spend * 0.01 * 50:,.0f} additional conversions** per week at current CPC.
That's the opportunity cost of leaving underperforming campaigns unaddressed.

*Auto-generated · {pd.Timestamp.today().strftime('%B %d, %Y')} · Google Ads Campaign Health Intelligence Pipeline*
"""
    st.markdown(brief)

    st.divider()
    st.subheader("Weekly ROAS Trend")
    weekly = (fact_df.groupby(fact_df["date"].dt.to_period("W").dt.start_time)
              .agg(spend=("spend_usd", "sum"), revenue=("revenue_usd", "sum"))
              .reset_index())
    weekly.columns = ["week_start", "spend", "revenue"]
    weekly["roas"] = weekly["revenue"] / weekly["spend"]
    fig_r = px.line(weekly, x="week_start", y="roas", markers=True,
                    labels={"roas": "ROAS", "week_start": "Week"})
    fig_r.add_hline(y=1.0, line_dash="dash", line_color="red",
                    annotation_text="Break-even ROAS = 1.0")
    fig_r.update_layout(height=300, plot_bgcolor="white", paper_bgcolor="white")
    st.plotly_chart(fig_r, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Spend by Geo")
        geo_spend = last_7.groupby("geo")["spend_usd"].sum().reset_index().sort_values("spend_usd", ascending=False)
        fig_g = px.bar(geo_spend, x="geo", y="spend_usd", color="spend_usd",
                       color_continuous_scale="Blues")
        fig_g.update_layout(height=300, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig_g, use_container_width=True)

    with col_b:
        st.subheader("ROAS by Device")
        device_roas = (last_7.groupby("device")
                       .agg(spend=("spend_usd", "sum"), revenue=("revenue_usd", "sum"))
                       .reset_index())
        device_roas["roas"] = device_roas["revenue"] / device_roas["spend"]
        fig_d = px.bar(device_roas, x="device", y="roas", color="roas",
                       color_continuous_scale="RdYlGn")
        fig_d.add_hline(y=1.0, line_dash="dash", line_color="gray")
        fig_d.update_layout(height=300, plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig_d, use_container_width=True)