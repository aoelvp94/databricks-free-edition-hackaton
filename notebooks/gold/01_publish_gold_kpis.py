# Databricks notebook source
# MAGIC %md
# MAGIC # Gold – KPIs and tables for BI / Agents

# MAGIC This notebook consolidates Silver-layer tables into Gold metrics and views. It runs as an
# MAGIC independent DLT pipeline to keep life cycles decoupled.

# COMMAND ----------

from datetime import datetime

import dlt
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

TARGET_CATALOG = "workspace"
SILVER_SCHEMA = spark.conf.get("source.silver_schema", "silver_travel")


def read_silver(table_name: str) -> DataFrame:
    full_name = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.{table_name}"
    return spark.read.table(full_name)


RUN_TS = F.current_timestamp()
RUN_DATE = F.to_date(RUN_TS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold tables

# COMMAND ----------

@dlt.table(
    name="gold_price_volatility",
    comment="Price volatility index by route and week to monitor opportunities.",
    table_properties={"quality": "gold", "business_unit": "travel_hacking"},
)
def gold_price_volatility() -> DataFrame:
    flights = read_silver("fact_flight_quote").select(
        "route_sk",
        "route_id",
        "purchase_date_sk",
        "total_price_usd",
        "base_price_usd",
    )
    route_dim = read_silver("dim_route").select("route_sk", "origin_city", "destination_city")
    time_dim = read_silver("dim_time").select(
        "date_sk",
        "date",
        "week_of_year",
        "year",
    )

    enriched = (
        flights.join(route_dim, "route_sk", "left")
        .join(time_dim.alias("purchase"), F.col("purchase_date_sk") == F.col("purchase.date_sk"), "left")
        .withColumn("price_ratio", F.col("total_price_usd") / F.col("base_price_usd"))
        .withColumn("purchase_week_of_year", F.col("purchase.week_of_year"))
        .withColumn("purchase_year", F.col("purchase.year"))
        .drop(
            F.col("purchase.date"),
            F.col("purchase.week_of_year"),
            F.col("purchase.year"),
            F.col("purchase.date_sk"),
        )
    )

    return (
        enriched.groupBy(
            "route_sk",
            "route_id",
            "origin_city",
            "destination_city",
            "purchase_week_of_year",
            "purchase_year",
        )
        .agg(
            F.avg("total_price_usd").alias("avg_price_usd"),
            F.stddev_pop("total_price_usd").alias("std_price_usd"),
            F.avg("price_ratio").alias("avg_price_ratio"),
            F.expr("percentile(total_price_usd, 0.1)").alias("p10_price_usd"),
            F.expr("percentile(total_price_usd, 0.9)").alias("p90_price_usd"),
        )
        .withColumn("volatility_index", F.round(F.col("std_price_usd") / F.col("avg_price_usd") * 100, 2))
        .withColumn("run_date", RUN_DATE)
    )


@dlt.table(
    name="gold_opportunity_index",
    comment="Final 0-100 score combining price, availability, and event signals.",
    table_properties={"quality": "gold", "business_unit": "travel_hacking"},
)
def gold_opportunity_index() -> DataFrame:
    combos = read_silver("fact_combo_offer")
    flights = read_silver("fact_flight_quote").select(
        F.col("quote_id").alias("flight_quote_id"),
        F.col("total_price_usd").alias("flight_total_price_usd"),
        F.col("availability_ratio").alias("flight_availability_ratio"),
        F.col("promotion_flag").alias("flight_promotion_flag"),
        "yield_per_km",
    )
    hotels = read_silver("fact_hotel_quote").select(
        F.col("quote_id").alias("hotel_quote_id"),
        F.col("avg_price_per_night").alias("hotel_avg_price_per_night"),
        F.col("availability_ratio").alias("hotel_availability_ratio"),
        F.col("promotion_flag").alias("hotel_promotion_flag"),
    )

    joined = (
        combos.join(flights, "flight_quote_id", "left")
        .join(hotels, "hotel_quote_id", "left")
        .withColumn("price_pressure", F.col("bundle_price_usd") / F.col("flight_total_price_usd"))
        .withColumn(
            "availability_signal",
            1
            - (
                F.coalesce(F.col("flight_availability_ratio"), F.lit(0.5))
                + F.coalesce(F.col("hotel_availability_ratio"), F.lit(0.5))
            )
            / 2,
        )
    )

    return (
        joined.select(
            "combo_sk",
            "flight_quote_id",
            "hotel_quote_id",
            "route_id",
            "carrier_code",
            "travel_date",
            "bundle_price_usd",
            "event_uplift",
            "event_type",
            "flight_total_price_usd",
            "hotel_avg_price_per_night",
            "flight_availability_ratio",
            "hotel_availability_ratio",
            F.round(
                F.col("opportunity_index_baseline")
                + (1 - F.col("price_pressure")) * 30
                + F.col("event_uplift") * 25
                + F.when(F.col("flight_promotion_flag") == 1, 8).otherwise(0)
                + F.when(F.col("hotel_promotion_flag") == 1, 5).otherwise(0),
                2,
            ).alias("opportunity_index"),
            RUN_TS.alias("computed_at"),
        )
        .withColumn("opportunity_index", F.expr("greatest(0, least(opportunity_index, 100))"))
    )


@dlt.table(
    name="gold_demand_forecast",
    comment="Simple forecast based on a 7-day moving average. Refined by the ML job.",
    table_properties={"quality": "gold", "business_unit": "travel_hacking"},
)
def gold_demand_forecast() -> DataFrame:
    flights = read_silver("fact_flight_quote")
    time_dim = read_silver("dim_time").select("date_sk", "date")

    daily = (
        flights.join(time_dim, flights.departure_date_sk == time_dim.date_sk, "left")
        .groupBy("route_id", "date")
        .agg(
            F.countDistinct("quote_id").alias("quotes_count"),
            F.avg("total_price_usd").alias("avg_price_usd"),
        )
    )

    window_spec = Window.partitionBy("route_id").orderBy("date").rowsBetween(-6, 0)

    return (
        daily.select(
            "route_id",
            F.col("date").alias("travel_date"),
            "quotes_count",
            F.round(F.avg("quotes_count").over(window_spec), 2).alias("rolling7_quotes_avg"),
            F.round(F.avg("avg_price_usd").over(window_spec), 2).alias("rolling7_price_avg"),
        )
        .withColumn(
            "forecast_quotes",
            F.round(F.col("rolling7_quotes_avg") * F.uniform(0.95, 1.08), 2),
        )
        .withColumn("forecast_generated_at", RUN_TS)
    )


@dlt.table(
    name="gold_predictions",
    comment="Target table to receive ML batch scoring outputs (MLflow).",
    table_properties={"quality": "gold", "business_unit": "travel_hacking"},
)
def gold_predictions() -> DataFrame:
    # Table starts empty; the batch scoring job will upsert records via merge.
    schema = "route_id STRING, travel_date DATE, opportunity_score DOUBLE, uplift DOUBLE, model_version STRING, scored_at TIMESTAMP"
    empty_df = spark.createDataFrame([], schema)
    return empty_df


