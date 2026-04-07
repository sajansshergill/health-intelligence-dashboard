# Methodology — Google Ads Campaign Health Intelligence Pipeline

## What is this project?

This pipeline detects campaign health degradation, CPC/ROAS anomalies, and creative variant lift
across a simulated Google Ads advertiser portfolio. It is designed to mirror the internal BI
infrastructure a Google Ads Growth Analytics team would build to support campaign managers
and account strategists.

---

## Why these anomaly thresholds?

**CPC anomaly: > 1.5x 7-day rolling average**
Google's Smart Bidding documentation states that tCPA and tROAS strategies are granted
a ±30% bid headroom on any given auction. A CPC exceeding 1.5x the rolling baseline
indicates the bidding algorithm is operating outside normal variance — a signal of
auction instability, quality score degradation, or keyword-level competition spike.
Source: Google Ads Help Center — "How target CPA bidding works"

**ROAS anomaly: < 0.5x 7-day rolling average**
A ROAS drop below 50% of the trailing 7-day average is used as the threshold because
Google's own Performance Max diagnostics flag campaigns as "Limited by conversion value"
when ROAS falls below target by more than 40–50%.
Source: Google Ads Performance Max Best Practices Guide (2024)

**CTR anomaly: < 0.5x 7-day rolling average**
A 50% CTR drop from baseline indicates either ad relevance degradation, audience fatigue,
or Quality Score erosion — all of which trigger bid penalties in the Google Ads auction.
Source: WordStream Google Ads Benchmarks by Industry (2024)

---

## Why a 7-day rolling window?

Google's own Smart Bidding algorithms use a lookback window of 7–30 days for conversion
modeling. A 7-day rolling window aligns with the minimum stable signal period for
campaigns with moderate traffic volume (500–1000 clicks/week).

---

## How is the health score calculated?

Health score is a weighted composite tailored to each advertiser's optimization goal:

**direct_sales:** `(avg_roas × 10) - (anomaly_rate × 50) + (avg_cvr × 300)`
**lead_generation:** `(avg_cvr × 400) - (avg_cpc × 2) - (anomaly_rate × 50)`
**brand_awareness:** `(avg_ctr × 500) - (anomaly_rate × 50)`

Weights are calibrated so that anomaly_rate has a meaningful negative impact
regardless of goal, while the primary KPI contribution dominates under normal conditions.

---

## How is the A/B test significance determined?

Mann-Whitney U test (two-sided, α = 0.05). Non-parametric test chosen because
CTR and CVR distributions are right-skewed and non-normal — a standard t-test
would underestimate variance. This is consistent with how Google's internal
experimentation teams handle ad creative testing.
Source: Kohavi, R., Tang, D., Xu, Y. — "Trustworthy Online Controlled Experiments" (2020)

---

## What does "LLM-readable" mean in this context?

Every SQL query in `analytics/queries.sql` contains inline comments explaining:
- The business rationale for the query
- The Google Ads concept it maps to
- The source publication that informs the threshold

This makes the codebase interpretable by both human reviewers and AI assistants
performing code review, documentation generation, or automated insight narration.

---

## Frequently Asked Questions

**Q: What is a CPC anomaly in Google Ads?**
A: A CPC anomaly occurs when the cost-per-click on a given day exceeds 1.5x the
7-day rolling average for that campaign, indicating auction instability or Smart
Bidding overcorrection.

**Q: How does this pipeline define campaign health?**
A: Campaign health is a composite score weighted by the advertiser's optimization
goal — ROAS for direct sales, CVR for lead generation, CTR for brand awareness —
penalized by anomaly rate. Campaigns are classified RED (>15% anomaly rate),
AMBER (7–15%), or GREEN (<7%).

**Q: Why use DuckDB instead of BigQuery?**
A: DuckDB provides an analytically equivalent columnar execution engine that runs
locally without infrastructure cost, making it ideal for portfolio projects that
demonstrate BigQuery-equivalent SQL patterns.

**Q: What real-world problem does this solve?**
A: Google Ads account managers monitoring hundreds of campaigns manually cannot
detect performance degradation before it compounds. This pipeline automates anomaly
detection, surfaces it in a self-serve dashboard, and provides LLM-generated
recommendations — reducing mean time to insight from days to minutes.