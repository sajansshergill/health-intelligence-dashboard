from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import duckdb
import pandas as pd
import os

os.makedirs("data/warehouse", exist_ok=True)
DB_PATH = "data/warehouse/ads_intelligence.duckdb"
RAW_PATH = "data/raw"


def create_spark():
    return (SparkSession.builder
            .appName("GoogleAdsCampaignETL")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())


def load_raw(spark):
    advertisers = spark.read.csv(f"{RAW_PATH}/dim_advertisers.csv", header=True, inferSchema=True)
    campaigns = spark.read.csv(f"{RAW_PATH}/dim_campaigns.csv", header=True, inferSchema=True)
    performance = spark.read.csv(f"{RAW_PATH}/fact_campaign_daily_raw.csv", header=True, inferSchema=True)
    return advertisers, campaigns, performance


def build_dim_date(performance_df):
    return (performance_df.select("date").distinct()
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
    )


def build_fact(performance_df):
    w7 = (Window
          .partitionBy("campaign_id")
          .orderBy(F.col("date").cast("timestamp").cast("long"))
          .rangeBetween(-6 * 86400, 0))

    return (performance_df
        .withColumn("rolling_7d_ctr",  F.avg("ctr").over(w7))
        .withColumn("rolling_7d_cpc",  F.avg("cpc").over(w7))
        .withColumn("rolling_7d_cvr",  F.avg("cvr").over(w7))
        .withColumn("rolling_7d_roas", F.avg("roas").over(w7))
        .withColumn("cpc_anomaly_flag",  F.col("cpc")  > F.col("rolling_7d_cpc")  * 1.5)
        .withColumn("ctr_anomaly_flag",  F.col("ctr")  < F.col("rolling_7d_ctr")  * 0.5)
        .withColumn("roas_anomaly_flag", F.col("roas") < F.col("rolling_7d_roas") * 0.5)
        .withColumn("record_id", F.expr("uuid()"))
    )


def build_mart_health(fact_df, campaigns_df, advertisers_df):
    # Fact and campaigns both carry advertiser_id / optimization_goal — drop from fact
    # so joins do not create ambiguous duplicate column names.
    fact_df = fact_df.drop("advertiser_id", "optimization_goal")
    joined = (fact_df
        .join(campaigns_df.select(
            "campaign_id", "campaign_name", "campaign_type",
            "bid_strategy", "creative_variant", "optimization_goal", "advertiser_id"),
            on="campaign_id", how="left")
        .join(advertisers_df.select(
            "advertiser_id", "advertiser_name", "industry",
            "primary_kpi", "client_id"),
            on="advertiser_id", how="left"))

    agg = (joined.groupBy(
            "campaign_id", "campaign_name", "advertiser_name", "industry",
            "optimization_goal", "primary_kpi", "campaign_type",
            "bid_strategy", "creative_variant", "client_id")
        .agg(
            F.avg("ctr").alias("avg_ctr"),
            F.avg("cpc").alias("avg_cpc"),
            F.avg("cvr").alias("avg_cvr"),
            F.avg("roas").alias("avg_roas"),
            F.sum("spend_usd").alias("total_spend_usd"),
            F.sum("revenue_usd").alias("total_revenue_usd"),
            F.sum("conversions").alias("total_conversions"),
            F.sum(F.col("cpc_anomaly_flag").cast("int")).alias("anomaly_days"),
            F.count("*").alias("total_days")
        )
        .withColumn("anomaly_rate", F.col("anomaly_days") / F.col("total_days"))
        # Health score weighted by optimization goal
        .withColumn("health_score",
            F.when(F.col("optimization_goal") == "direct_sales",
                (F.col("avg_roas") * 10)
                - (F.col("anomaly_rate") * 50)
                + (F.col("avg_cvr") * 300))
            .when(F.col("optimization_goal") == "lead_generation",
                (F.col("avg_cvr") * 400)
                - (F.col("avg_cpc") * 2)
                - (F.col("anomaly_rate") * 50))
            .otherwise(
                (F.col("avg_ctr") * 500)
                - (F.col("anomaly_rate") * 50))
        )
        .withColumn("rag_status",
            F.when(F.col("anomaly_rate") > 0.15, "RED")
             .when(F.col("anomaly_rate") > 0.07, "AMBER")
             .otherwise("GREEN"))
    )
    return agg


_WAREHOUSE_DROP_ORDER = (
    "fact_campaign_daily",
    "mart_campaign_health",
    "dim_campaigns",
    "dim_date",
    "dim_advertisers",
)


def write_to_duckdb(dataframes: dict):
    con = duckdb.connect(DB_PATH)
    for table_name in _WAREHOUSE_DROP_ORDER:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
    for table_name, df in dataframes.items():
        pdf = df.toPandas()
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM pdf")
        print(f"  {len(pdf):,} rows → {table_name}")
    con.close()


def main():
    print("Starting Spark...")
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    advertisers, campaigns, performance = load_raw(spark)
    dim_date = build_dim_date(performance)
    fact = build_fact(performance)
    mart = build_mart_health(fact, campaigns, advertisers)

    write_to_duckdb({
        "dim_advertisers": advertisers,
        "dim_campaigns": campaigns,
        "dim_date": dim_date,
        "fact_campaign_daily": fact,
        "mart_campaign_health": mart
    })

    spark.stop()
    print(f"\nWarehouse ready: {DB_PATH}")


if __name__ == "__main__":
    main()