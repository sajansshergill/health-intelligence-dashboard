# 🎯 Google Ads Campaign Health Intelligence Dashboard

A production-grade BI pipeline that monitors advertiser campaign performance, detects anomalies, and surfaces A/B test lift — built to mirror Google Ads Internal reporting workflows.

## Overview
This project simulates the analytics infrastructure a Google BI Analyst would build to support the Ads Growth team. It ingests synthetic campaign performance data, models it into a dimensional warehouse, and delivers a self-serve Streamlit dashboard with health scoring, anomaly detection, and experimentation analysis.

## Tech Stack
<img width="619" height="269" alt="image" src="https://github.com/user-attachments/assets/037c9fe8-da95-4a67-98e7-f64d85057e59" />

## Features
- **Campaign Health Scorecard** - CTR trend, CPC anomaly flag, CVR vs 7-day rolling baseline, RAG status per campaign
- **Dimension Data Warehouse** - fact_campaign_daily + dim tables for advertiser, geo, device, keyword
- **A/B Test Lift Analyzer** - Compare creative variants on CTR and CVR with p-value, CI, and plain-English winner recommendation
- **Executive Summary Tab** - Auto-generated insight blurbs for non-technical stakeholders

## Project Structure
google-ads-bi-dashboard/
├── data/
│   └── generate_campaigns.py
├── etl/
│   └── pyspark_pipeline.py
├── warehouse/
│   └── schema.sql
├── analytics/
│   └── queries.sql
├── app/
│   └── streamlit_app.py
└── README.md

## Getting Started
pip install -r requirements.txt
python data/generate_campaigns.py
python etl/pyspark_pipeline.py
streamlit run app/streamlit_app.py


## Key SQL Patterns
-- 7-day roling CTR baseline per campaign
SELECT
  campaign_id,
  date,
  ctr,
  AVG(ctr) OVER (
    PARTITION BY campaign_id
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7d_ctr
FROM fact_campaign_daily;

## Business Context
Built to demonstrate the core workflow of a Google Ads BI Analyst: monitor campaign health at scale, surface anomalies before they impact advertiser ROI, and enable non-technical stakeholders to self-serve insights without waiting for ad-hoc requests.
