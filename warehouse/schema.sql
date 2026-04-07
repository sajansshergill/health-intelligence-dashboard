-- ═══════════════════════════════════════════════════════
-- Multi-tenant dimensional schema
-- Partitioned by client_id for B2B2C scaling
-- ═══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_advertisers (
    advertiser_id       VARCHAR PRIMARY KEY,
    advertiser_name     VARCHAR,
    industry            VARCHAR,
    monthly_budget_usd  DOUBLE,
    optimization_goal   VARCHAR,   -- brand_awareness | lead_generation | direct_sales
    primary_kpi         VARCHAR,   -- impressions | cvr | roas
    client_id           VARCHAR DEFAULT 'default'  -- B2B2C tenant key
);

CREATE TABLE IF NOT EXISTS dim_campaigns (
    campaign_id         VARCHAR PRIMARY KEY,
    campaign_name       VARCHAR,
    advertiser_id       VARCHAR,
    campaign_type       VARCHAR,   -- Search | Performance Max | Display | Shopping | Video
    bid_strategy        VARCHAR,   -- target_roas | target_cpa | maximize_conversions | etc.
    daily_budget_usd    DOUBLE,
    creative_variant    VARCHAR,
    optimization_goal   VARCHAR,
    FOREIGN KEY (advertiser_id) REFERENCES dim_advertisers(advertiser_id)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date            DATE PRIMARY KEY,
    year            INTEGER,
    month           INTEGER,
    month_name      VARCHAR,
    week            INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR,
    is_weekend      BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_campaign_daily (
    record_id               VARCHAR PRIMARY KEY,
    campaign_id             VARCHAR,
    advertiser_id           VARCHAR,
    optimization_goal       VARCHAR,
    date                    DATE,
    device                  VARCHAR,
    geo                     VARCHAR,
    impressions             BIGINT,
    clicks                  BIGINT,
    ctr                     DOUBLE,
    cpc                     DOUBLE,
    spend_usd               DOUBLE,
    conversions             BIGINT,
    cvr                     DOUBLE,
    revenue_usd             DOUBLE,
    roas                    DOUBLE,
    rolling_7d_ctr          DOUBLE,
    rolling_7d_cpc          DOUBLE,
    rolling_7d_cvr          DOUBLE,
    rolling_7d_roas         DOUBLE,
    cpc_anomaly_flag        BOOLEAN,
    ctr_anomaly_flag        BOOLEAN,
    roas_anomaly_flag       BOOLEAN,
    is_anomaly_injected     BOOLEAN,
    FOREIGN KEY (campaign_id) REFERENCES dim_campaigns(campaign_id)
);

CREATE TABLE IF NOT EXISTS mart_campaign_health (
    campaign_id             VARCHAR PRIMARY KEY,
    campaign_name           VARCHAR,
    advertiser_name         VARCHAR,
    industry                VARCHAR,
    optimization_goal       VARCHAR,
    primary_kpi             VARCHAR,
    campaign_type           VARCHAR,
    bid_strategy            VARCHAR,
    creative_variant        VARCHAR,
    avg_ctr                 DOUBLE,
    avg_cpc                 DOUBLE,
    avg_cvr                 DOUBLE,
    avg_roas                DOUBLE,
    total_spend_usd         DOUBLE,
    total_revenue_usd       DOUBLE,
    total_conversions       BIGINT,
    anomaly_days            BIGINT,
    total_days              BIGINT,
    anomaly_rate            DOUBLE,
    health_score            DOUBLE,
    rag_status              VARCHAR,
    client_id               VARCHAR DEFAULT 'default'
);