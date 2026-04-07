-- ═══════════════════════════════════════════════════════
-- Google Ads Campaign Health Intelligence — Analytical Queries
-- Thresholds sourced from: WordStream Google Ads Benchmarks 2024,
-- Think with Google Performance Insights, and Google Ads Help Center
-- ═══════════════════════════════════════════════════════


-- ─────────────────────────────────────────
-- Q1: 7-day rolling CTR baseline per campaign
-- Why: CTR benchmarks vary by industry (2–9%). Rolling avg smooths
-- daily noise and surfaces structural underperformance.
-- Source: WordStream Industry Benchmark CTR ranges 2024
-- ─────────────────────────────────────────
SELECT
    campaign_id,
    date,
    ctr,
    ROUND(AVG(ctr) OVER (
        PARTITION BY campaign_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 4) AS rolling_7d_ctr,
    ROUND(ctr - AVG(ctr) OVER (
        PARTITION BY campaign_id
        ORDER BY date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 4) AS ctr_delta_from_baseline
FROM fact_campaign_daily
ORDER BY campaign_id, date;


-- ─────────────────────────────────────────
-- Q2: CPC anomaly detection — spikes > 1.5x 7-day rolling average
-- Why: Smart Bidding can cause CPC spikes during auction volatility.
-- 1.5x threshold derived from Google's own tCPA guidance on bid headroom.
-- ─────────────────────────────────────────
WITH rolling AS (
    SELECT
        campaign_id,
        date,
        cpc,
        AVG(cpc) OVER (
            PARTITION BY campaign_id
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_cpc
    FROM fact_campaign_daily
)
SELECT
    campaign_id,
    date,
    ROUND(cpc, 2)             AS cpc,
    ROUND(rolling_7d_cpc, 2)  AS rolling_7d_cpc,
    ROUND(cpc / NULLIF(rolling_7d_cpc, 0), 2) AS cpc_ratio,
    CASE WHEN cpc > rolling_7d_cpc * 1.5 THEN 'ANOMALY' ELSE 'NORMAL' END AS status
FROM rolling
WHERE cpc > rolling_7d_cpc * 1.5
ORDER BY cpc_ratio DESC;


-- ─────────────────────────────────────────
-- Q3: ROAS performance by campaign type
-- Why: Performance Max campaigns typically deliver higher ROAS
-- than Search alone due to cross-channel inventory access.
-- Source: Google Performance Max Best Practices Guide 2024
-- ─────────────────────────────────────────
SELECT
    c.campaign_type,
    COUNT(DISTINCT f.campaign_id)           AS campaigns,
    ROUND(AVG(f.roas), 2)                   AS avg_roas,
    ROUND(AVG(f.ctr), 4)                    AS avg_ctr,
    ROUND(AVG(f.cvr), 4)                    AS avg_cvr,
    ROUND(SUM(f.spend_usd), 2)              AS total_spend,
    ROUND(SUM(f.revenue_usd), 2)            AS total_revenue,
    RANK() OVER (ORDER BY AVG(f.roas) DESC) AS roas_rank
FROM fact_campaign_daily f
JOIN dim_campaigns c ON f.campaign_id = c.campaign_id
GROUP BY c.campaign_type
ORDER BY avg_roas DESC;


-- ─────────────────────────────────────────
-- Q4: Goal-based primary KPI performance
-- Why: Advertisers optimizing for brand_awareness should be
-- evaluated on impressions/CTR, not CVR. Conflating KPIs
-- misrepresents campaign health.
-- ─────────────────────────────────────────
SELECT
    f.optimization_goal,
    COUNT(DISTINCT f.campaign_id)           AS campaigns,
    ROUND(AVG(f.impressions), 0)            AS avg_impressions,
    ROUND(AVG(f.ctr), 4)                    AS avg_ctr,
    ROUND(AVG(f.cvr), 4)                    AS avg_cvr,
    ROUND(AVG(f.roas), 2)                   AS avg_roas,
    ROUND(SUM(f.spend_usd), 2)              AS total_spend,
    ROUND(SUM(f.revenue_usd), 2)            AS total_revenue
FROM fact_campaign_daily f
GROUP BY f.optimization_goal
ORDER BY total_spend DESC;


-- ─────────────────────────────────────────
-- Q5: A/B creative variant lift by optimization goal
-- Why: Variant performance differs by goal — a Variant B that wins
-- on CVR may lose on ROAS for direct_sales campaigns.
-- ─────────────────────────────────────────
SELECT
    c.creative_variant,
    f.optimization_goal,
    COUNT(DISTINCT f.campaign_id)   AS num_campaigns,
    ROUND(AVG(f.ctr), 4)            AS avg_ctr,
    ROUND(AVG(f.cvr), 4)            AS avg_cvr,
    ROUND(AVG(f.roas), 2)           AS avg_roas,
    ROUND(AVG(f.cpc), 2)            AS avg_cpc,
    ROUND(SUM(f.spend_usd), 2)      AS total_spend,
    SUM(f.conversions)              AS total_conversions
FROM fact_campaign_daily f
JOIN dim_campaigns c ON f.campaign_id = c.campaign_id
GROUP BY c.creative_variant, f.optimization_goal
ORDER BY f.optimization_goal, avg_cvr DESC;


-- ─────────────────────────────────────────
-- Q6: Weekly spend trend with WoW % change
-- ─────────────────────────────────────────
WITH weekly AS (
    SELECT
        DATE_TRUNC('week', date)     AS week_start,
        ROUND(SUM(spend_usd), 2)     AS weekly_spend,
        ROUND(SUM(revenue_usd), 2)   AS weekly_revenue
    FROM fact_campaign_daily
    GROUP BY DATE_TRUNC('week', date)
)
SELECT
    week_start,
    weekly_spend,
    weekly_revenue,
    ROUND(weekly_revenue / NULLIF(weekly_spend, 0), 2) AS weekly_roas,
    LAG(weekly_spend) OVER (ORDER BY week_start)       AS prev_week_spend,
    ROUND(
        (weekly_spend - LAG(weekly_spend) OVER (ORDER BY week_start))
        / NULLIF(LAG(weekly_spend) OVER (ORDER BY week_start), 0) * 100, 2
    ) AS wow_spend_pct_change
FROM weekly
ORDER BY week_start;


-- ─────────────────────────────────────────
-- Q7: Device performance breakdown
-- ─────────────────────────────────────────
SELECT
    device,
    ROUND(AVG(ctr), 4)                                     AS avg_ctr,
    ROUND(AVG(cpc), 2)                                     AS avg_cpc,
    ROUND(AVG(cvr), 4)                                     AS avg_cvr,
    ROUND(AVG(roas), 2)                                    AS avg_roas,
    ROUND(SUM(spend_usd), 2)                               AS total_spend,
    SUM(conversions)                                       AS total_conversions,
    ROUND(SUM(spend_usd) / NULLIF(SUM(conversions), 0), 2) AS cost_per_conversion
FROM fact_campaign_daily
GROUP BY device
ORDER BY total_spend DESC;


-- ─────────────────────────────────────────
-- Q8: Geo-level performance
-- ─────────────────────────────────────────
SELECT
    geo,
    ROUND(AVG(ctr), 4)                                     AS avg_ctr,
    ROUND(AVG(cpc), 2)                                     AS avg_cpc,
    ROUND(AVG(cvr), 4)                                     AS avg_cvr,
    ROUND(AVG(roas), 2)                                    AS avg_roas,
    ROUND(SUM(spend_usd), 2)                               AS total_spend,
    SUM(conversions)                                       AS total_conversions,
    ROUND(SUM(spend_usd) / NULLIF(SUM(conversions), 0), 2) AS cost_per_conversion
FROM fact_campaign_daily
GROUP BY geo
ORDER BY total_spend DESC;


-- ─────────────────────────────────────────
-- Q9: Anomaly rate by bid strategy
-- ─────────────────────────────────────────
SELECT
    c.bid_strategy,
    COUNT(*)                                                                AS total_days,
    SUM(CASE WHEN f.cpc_anomaly_flag THEN 1 ELSE 0 END)                    AS anomaly_days,
    ROUND(SUM(CASE WHEN f.cpc_anomaly_flag THEN 1 ELSE 0 END) * 100.0
          / COUNT(*), 2)                                                    AS anomaly_rate_pct,
    ROUND(AVG(f.roas), 2)                                                   AS avg_roas
FROM fact_campaign_daily f
JOIN dim_campaigns c ON f.campaign_id = c.campaign_id
GROUP BY c.bid_strategy
ORDER BY anomaly_rate_pct DESC;


-- ─────────────────────────────────────────
-- Q10: Executive snapshot — last 7 days
-- ─────────────────────────────────────────
SELECT
    COUNT(DISTINCT campaign_id)                             AS active_campaigns,
    ROUND(SUM(spend_usd), 2)                               AS total_spend_7d,
    ROUND(SUM(revenue_usd), 2)                             AS total_revenue_7d,
    ROUND(SUM(revenue_usd) / NULLIF(SUM(spend_usd), 0), 2) AS overall_roas_7d,
    ROUND(AVG(ctr), 4)                                     AS avg_ctr_7d,
    ROUND(AVG(cpc), 2)                                     AS avg_cpc_7d,
    ROUND(AVG(cvr), 4)                                     AS avg_cvr_7d,
    SUM(conversions)                                       AS total_conversions_7d,
    SUM(CASE WHEN cpc_anomaly_flag  THEN 1 ELSE 0 END)     AS cpc_anomaly_days,
    SUM(CASE WHEN roas_anomaly_flag THEN 1 ELSE 0 END)     AS roas_anomaly_days
FROM fact_campaign_daily
WHERE date >= CURRENT_DATE - INTERVAL 7 DAY;