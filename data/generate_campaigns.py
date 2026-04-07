import pandas as pd
import numpy as np
from faker import Faker
import random
import os

fake = Faker()
random.seed(42)
np.random.seed(42)

# Google Ads authentic terminology
INDUSTRIES = ["Retail", "Finance", "Healthcare", "Travel", "Tech", "Food & Beverage", "Education", "Automotive"]
DEVICES = ["mobile", "desktop", "tablet"]
GEOS = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "San Francisco", "Seattle", "Boston"]

# Real Google Ads bid strategy names
BID_STRATEGIES = ["target_roas", "target_cpa", "maximize_conversions", "maximize_conversion_value", "manual_cpc"]

# Real Google Ads campaign types
CAMPAIGN_TYPES = ["Search", "Performance Max", "Display", "Shopping", "Video"]

CREATIVE_VARIANTS = ["A", "B"]

# Advertiser optimization goals — drives personalization layer
OPTIMIZATION_GOALS = ["brand_awareness", "lead_generation", "direct_sales"]

# Industry-realistic CTR benchmarks sourced from WordStream / Google Ads benchmarks
# Source: WordStream Google Ads Industry Benchmarks 2024
INDUSTRY_CTR_BENCHMARKS = {
    "Retail": 0.077,
    "Finance": 0.056,
    "Healthcare": 0.059,
    "Travel": 0.090,
    "Tech": 0.051,
    "Food & Beverage": 0.074,
    "Education": 0.053,
    "Automotive": 0.060
}

INDUSTRY_CVR_BENCHMARKS = {
    "Retail": 0.037,
    "Finance": 0.054,
    "Healthcare": 0.032,
    "Travel": 0.028,
    "Tech": 0.023,
    "Food & Beverage": 0.041,
    "Education": 0.035,
    "Automotive": 0.061
}

NUM_ADVERTISERS = 50
NUM_CAMPAIGNS = 200
NUM_DAYS = 90

os.makedirs("data/raw", exist_ok=True)


def generate_advertisers():
    records = []
    for i in range(NUM_ADVERTISERS):
        industry = random.choice(INDUSTRIES)
        goal = random.choice(OPTIMIZATION_GOALS)

        # Budget realistic to goal — brand awareness campaigns spend more
        if goal == "brand_awareness":
            budget = round(random.uniform(50000, 500000), 2)
        elif goal == "lead_generation":
            budget = round(random.uniform(10000, 100000), 2)
        else:
            budget = round(random.uniform(5000, 80000), 2)

        records.append({
            "advertiser_id": f"ADV_{i+1:04d}",
            "advertiser_name": fake.company(),
            "industry": industry,
            "monthly_budget_usd": budget,
            "optimization_goal": goal,
            "primary_kpi": {
                "brand_awareness": "impressions",
                "lead_generation": "cvr",
                "direct_sales": "roas"
            }[goal],
            "client_id": "default",
        })
    return pd.DataFrame(records)


def generate_campaigns(advertisers_df):
    records = []
    for i in range(NUM_CAMPAIGNS):
        adv = advertisers_df.sample(1).iloc[0]
        goal = adv["optimization_goal"]

        # Bid strategy aligned to goal — mirrors real Google Ads logic
        if goal == "brand_awareness":
            bid_strategy = random.choice(["maximize_conversions", "target_cpa"])
        elif goal == "lead_generation":
            bid_strategy = random.choice(["target_cpa", "maximize_conversions", "manual_cpc"])
        else:
            bid_strategy = random.choice(["target_roas", "maximize_conversion_value"])

        records.append({
            "campaign_id": f"CAMP_{i+1:04d}",
            "campaign_name": f"{fake.bs().title()} — {random.choice(CAMPAIGN_TYPES)}",
            "advertiser_id": adv["advertiser_id"],
            "campaign_type": random.choice(CAMPAIGN_TYPES),
            "bid_strategy": bid_strategy,
            "daily_budget_usd": round(adv["monthly_budget_usd"] / 30 * random.uniform(0.5, 1.5), 2),
            "creative_variant": random.choice(CREATIVE_VARIANTS),
            "optimization_goal": goal
        })
    return pd.DataFrame(records)


def generate_daily_performance(campaigns_df, advertisers_df):
    records = []
    dates = pd.date_range(end=pd.Timestamp.today().normalize(), periods=NUM_DAYS)
    adv_lookup = advertisers_df.set_index("advertiser_id")

    for _, camp in campaigns_df.iterrows():
        adv = adv_lookup.loc[camp["advertiser_id"]]
        industry = adv["industry"]

        # Anchor to industry benchmarks for authenticity
        base_ctr = INDUSTRY_CTR_BENCHMARKS[industry] * random.uniform(0.7, 1.3)
        base_cvr = INDUSTRY_CVR_BENCHMARKS[industry] * random.uniform(0.7, 1.3)
        base_cpc = round(random.uniform(0.5, 8.0), 2)

        for date in dates:
            anomaly = random.random() < 0.05
            cpc_spike = random.uniform(1.8, 3.0) if anomaly else 1.0
            ctr_drop = random.uniform(0.3, 0.6) if anomaly else 1.0

            impressions = int(random.uniform(1000, 50000))
            ctr = round(base_ctr * ctr_drop * random.uniform(0.85, 1.15), 4)
            clicks = int(impressions * ctr)
            cpc = round(base_cpc * cpc_spike * random.uniform(0.9, 1.1), 2)
            spend = round(clicks * cpc, 2)
            cvr = round(base_cvr * random.uniform(0.8, 1.2), 4)
            conversions = int(clicks * cvr)
            revenue = round(conversions * random.uniform(20, 500), 2)
            roas = round(revenue / spend, 2) if spend > 0 else 0.0

            records.append({
                "campaign_id": camp["campaign_id"],
                "advertiser_id": camp["advertiser_id"],
                "optimization_goal": camp["optimization_goal"],
                "date": date.date(),
                "device": random.choice(DEVICES),
                "geo": random.choice(GEOS),
                "impressions": impressions,
                "clicks": clicks,
                "ctr": ctr,
                "cpc": cpc,
                "spend_usd": spend,
                "conversions": conversions,
                "cvr": cvr,
                "revenue_usd": revenue,
                "roas": roas,
                "is_anomaly_injected": anomaly
            })

    return pd.DataFrame(records)


def main():
    print("Generating advertisers...")
    advertisers = generate_advertisers()
    advertisers.to_csv("data/raw/dim_advertisers.csv", index=False)

    print("Generating campaigns...")
    campaigns = generate_campaigns(advertisers)
    campaigns.to_csv("data/raw/dim_campaigns.csv", index=False)

    print("Generating daily performance...")
    performance = generate_daily_performance(campaigns, advertisers)
    performance.to_csv("data/raw/fact_campaign_daily_raw.csv", index=False)
    print(f"  {len(performance):,} records written to data/raw/")

    print("\nDone.")


if __name__ == "__main__":
    main()