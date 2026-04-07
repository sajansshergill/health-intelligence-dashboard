# Learnings Log — Google Ads Campaign Health Intelligence Pipeline

## What I knew before building this

- PySpark basics, DuckDB SQL, Streamlit layout
- Standard A/B testing with t-tests
- Streamlit dashboard structure

---

## What I had to learn to build this

**Google Ads bidding mechanics**
I had no prior knowledge of how tCPA, tROAS, or Performance Max actually work
under the hood. I read Google's Smart Bidding documentation and the Performance Max
Best Practices Guide to understand what bid headroom means and why 1.5x CPC is a
meaningful anomaly threshold — not an arbitrary one.

**Non-parametric testing for skewed distributions**
My initial implementation used a t-test for the A/B analyzer. After reviewing the
CTR distribution (right-skewed, non-normal), I switched to Mann-Whitney U.
This came from reading Kohavi et al.'s "Trustworthy Online Controlled Experiments" —
a book I would not have picked up without this project forcing the question.

**Goal-based KPI personalization in dashboards**
I had never built a dashboard where the primary metric changes based on user context.
Implementing the optimization_goal → primary_kpi mapping required rethinking
the entire health score formula — not just the display layer.

**Calling the Anthropic API with structured business context**
Passing a JSON campaign context to an LLM and prompting it to respond in domain-specific
language (Google Ads terminology) required multiple prompt iterations. The key insight:
the more specific the context object, the more specific and actionable the recommendation.

**Multi-tenant data architecture**
Adding client_id as a partition key to support B2B2C scaling was architecturally simple
but conceptually important — it forced me to think about how a single pipeline
serves multiple agency clients, not just one advertiser.

---

## What I would do differently next time

- Use real Google Ads API data (via the Google Ads Python client library) instead of synthetic data
- Add dbt-style transformation layers with lineage documentation
- Implement incremental loading instead of full refresh on each ETL run
- Add Great Expectations data quality checks before the DuckDB write step